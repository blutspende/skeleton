package skeleton

import (
	"context"
	"skeleton/auth"
	"skeleton/clients"
	"skeleton/config"
	"skeleton/db"
	"skeleton/migrator"
	"skeleton/repositories"
	"skeleton/services"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

type skeleton struct {
	callBackHandler    SkeletonCallbackHandlerV1
	migrator           migrator.SkeletonMigrator
	analysisService    services.AnalysisService
	analysisRepository repositories.AnalysisRepository
	resultsBuffer      []AnalysisResultV1
	resultsChan        chan AnalysisResultV1
	resultBatchesChan  chan []AnalysisResultV1
	cerberusClient     clients.CerberusV1
}

func (s *skeleton) SetCallbackHandler(eventHandler skeletonapi.SkeletonCallbackHandlerV1) {
	s.callBackHandler = eventHandler
}
func (s *skeleton) GetCallbackHandler() skeletonapi.SkeletonCallbackHandlerV1 {
	return s.callBackHandler
}

func (s *skeleton) Log(instrumentID uuid.UUID, msg string) {
	log.Info().Interface("instrumentId", instrumentID).Msg(msg)
}

func (s *skeleton) LogError(instrumentID uuid.UUID, err error) {
	log.Error().Interface("instrumentId", instrumentID).Err(err).Msg("")
}

func (s *skeleton) LogDebug(instrumentID uuid.UUID, msg string) {
	log.Debug().Interface("instrumentId", instrumentID).Msg(msg)
}

func (s *skeleton) GetAnalysisRequestWithNoResults(currentPage, itemsPerPage int) (requests []skeletonapi.AnalysisRequestV1, maxPages int, err error) {

	return []skeletonapi.AnalysisRequestV1{}, 0, nil
}

func (s *skeleton) GetAnalysisRequestsBySampleCode(sampleCode string) ([]skeletonapi.AnalysisRequestV1, error) {
	return []skeletonapi.AnalysisRequestV1{}, nil
}

func (s *skeleton) GetAnalysisRequestsBySampleCodes(sampleCodes []string) ([]skeletonapi.AnalysisRequestV1, error) {
	return []skeletonapi.AnalysisRequestV1{}, nil
}

func (s *skeleton) GetRequestMappingsByInstrumentID(instrumentID uuid.UUID) ([]skeletonapi.RequestMappingV1, error) {
	return []skeletonapi.RequestMappingV1{}, nil
}

func (s *skeleton) SubmitAnalysisResult(ctx context.Context, resultData skeletonapi.AnalysisResultV1, submitTypes ...skeletonapi.SubmitType) error {
	tx, err := s.analysisRepository.CreateTransaction()
	if err != nil {
		return err
	}
	_, err = s.analysisRepository.WithTransaction(tx).CreateAnalysisResultsBatch(ctx, []skeletonapi.AnalysisResultV1{resultData})
	if err != nil {
		return err
	}

	s.resultsChan <- resultData
	return nil
}

func (s *skeleton) GetInstrument(instrumentID uuid.UUID) (skeletonapi.InstrumentV1, error) {
	return skeletonapi.InstrumentV1{}, nil
}

func (s *skeleton) GetInstruments() ([]skeletonapi.InstrumentV1, error) {
	return []skeletonapi.InstrumentV1{}, nil
}

func (s *skeleton) FindAnalyteByManufacturerTestCode(instrument skeletonapi.InstrumentV1, testCode string) skeletonapi.AnalyteMappingV1 {
	return skeletonapi.AnalyteMappingV1{}
}

func (s *skeleton) FindResultMapping(searchvalue string, mapping []skeletonapi.ResultMappingV1) (string, error) {
	return "", nil
}

func (s *skeleton) migrateUp(ctx context.Context, db *sqlx.DB, schemaName string) error {
	return s.migrator.Run(ctx, db, schemaName)
}

func (s *skeleton) Start(ctx context.Context, db *sqlx.DB, schemaName string) error {
	err := s.migrateUp(ctx, db, schemaName)
	if err != nil {
		return err
	}

	go s.processAnalysisResults(ctx)
	go s.processAnalysisResultBatches(ctx)
	return nil
}

func (s *skeleton) processAnalysisResults(ctx context.Context) {
	for {
		select {
		case result, ok := <-s.resultsChan:
			if !ok {
				log.Fatal().Msg("processing analysis results stopped: results channel closed")
			}
			s.resultsBuffer = append(s.resultsBuffer, result)
			if len(s.resultsBuffer) >= 500 {
				s.resultBatchesChan <- s.resultsBuffer
				s.resultsBuffer = make([]skeletonapi.AnalysisResultV1, 0, 500)
			}
		case <-time.After(3 * time.Second):
			s.resultBatchesChan <- s.resultsBuffer
			s.resultsBuffer = make([]skeletonapi.AnalysisResultV1, 0, 500)
		}
	}
}

const maxRetryCount = 30

func (s *skeleton) processAnalysisResultBatches(ctx context.Context) {
	for {
		resultsBatch, ok := <-s.resultBatchesChan
		if !ok {
			log.Fatal().Msg("processing analysis result batches stopped: resultBatches channel closed")
		}
		creationStatuses, err := s.cerberusClient.PostAnalysisResultBatch(resultsBatch)
		if err != nil {
			time.AfterFunc(30*time.Second, func() {
				s.resultBatchesChan <- resultsBatch
			})
			continue
		}
		for i, status := range creationStatuses {
			err = s.analysisRepository.UpdateResultTransmissionData(ctx, resultsBatch[i].ID, status.Success, status.ErrorMessage)
			if !status.Success && resultsBatch[i].RetryCount < maxRetryCount {
				time.AfterFunc(30*time.Second, func() {
					s.resultsChan <- resultsBatch[i]
				})
			}
		}
	}
}

func NewV1(migrator migrator.SkeletonMigrator, analysisService services.AnalysisService, analysisRepository repositories.AnalysisRepository, cerberusClient clients.CerberusV1) skeletonapi.SkeletonAPI {
	return &skeleton{
		migrator:           migrator,
		analysisService:    analysisService,
		analysisRepository: analysisRepository,
		cerberusClient:     cerberusClient,
		resultsBuffer:      make([]skeletonapi.AnalysisResultV1, 0, 500),
		resultsChan:        make(chan skeletonapi.AnalysisResultV1, 500),
		resultBatchesChan:  make(chan []skeletonapi.AnalysisResultV1, 10),
	}
}

func NewV1Default(sqlConn *sqlx.DB, dbSchema string, config *config.Configuration) (skeletonapi.SkeletonAPI, error) {
	authManager := auth.NewAuthManager(config,
		clients.NewRestyClient(context.Background(), config, true))
	authManager.StartClientCredentialTask(context.Background())
	internalApiRestyClient := clients.NewRestyClientWithAuthManager(context.Background(), config, authManager)
	cerberusClient, err := clients.NewCerberusV1Client(config.CerberusURL, internalApiRestyClient)
	analysisService := services.NewAnalysisService()
	if err != nil {
		return nil, err
	}
	dbConn := db.CreateDbConnector(sqlConn)
	return NewV1(migrator.NewSkeletonMigrator(), analysisService, repositories.NewAnalysisRepository(dbConn, dbSchema), cerberusClient), nil
}
