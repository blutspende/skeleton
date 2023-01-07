package services

import (
	"context"
	"skeleton/clients"
	"skeleton/config"
	"skeleton/db"
	"skeleton/migrator"
	"skeleton/repositories"
	v1 "skeleton/v1"
	"time"

	authmanager "skeleton/authmanager"

	"github.com/jmoiron/sqlx"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

type skeleton struct {
	callBackHandler    v1.SkeletonCallbackHandlerV1
	migrator           migrator.SkeletonMigrator
	analysisService    AnalysisService
	analysisRepository repositories.AnalysisRepository
	resultsBuffer      []v1.AnalysisResultV1
	resultsChan        chan v1.AnalysisResultV1
	resultBatchesChan  chan []v1.AnalysisResultV1
	cerberusClient     clients.CerberusV1
}

func (s *skeleton) SetCallbackHandler(eventHandler v1.SkeletonCallbackHandlerV1) {
	s.callBackHandler = eventHandler
}
func (s *skeleton) GetCallbackHandler() v1.SkeletonCallbackHandlerV1 {
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

func (s *skeleton) GetAnalysisRequestWithNoResults(currentPage, itemsPerPage int) (requests []v1.AnalysisRequestV1, maxPages int, err error) {

	return []v1.AnalysisRequestV1{}, 0, nil
}

func (s *skeleton) GetAnalysisRequestsBySampleCode(sampleCode string) ([]v1.AnalysisRequestV1, error) {
	return []v1.AnalysisRequestV1{}, nil
}

func (s *skeleton) GetAnalysisRequestsBySampleCodes(sampleCodes []string) ([]v1.AnalysisRequestV1, error) {
	return []v1.AnalysisRequestV1{}, nil
}

func (s *skeleton) GetRequestMappingsByInstrumentID(instrumentID uuid.UUID) ([]v1.RequestMappingV1, error) {
	return []v1.RequestMappingV1{}, nil
}

func (s *skeleton) SubmitAnalysisResult(ctx context.Context, resultData v1.AnalysisResultV1, submitTypes ...v1.SubmitType) error {
	tx, err := s.analysisRepository.CreateTransaction()
	if err != nil {
		return err
	}
	_, err = s.analysisRepository.WithTransaction(tx).CreateAnalysisResultsBatch(ctx, []v1.AnalysisResultV1{resultData})
	if err != nil {
		return err
	}

	s.resultsChan <- resultData
	return nil
}

func (s *skeleton) GetInstrument(instrumentID uuid.UUID) (v1.InstrumentV1, error) {
	return v1.InstrumentV1{}, nil
}

func (s *skeleton) GetInstruments() ([]v1.InstrumentV1, error) {
	return []v1.InstrumentV1{}, nil
}

func (s *skeleton) FindAnalyteByManufacturerTestCode(instrument v1.InstrumentV1, testCode string) v1.AnalyteMappingV1 {
	return v1.AnalyteMappingV1{}
}

func (s *skeleton) FindResultMapping(searchvalue string, mapping []v1.ResultMappingV1) (string, error) {
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
				s.resultsBuffer = make([]v1.AnalysisResultV1, 0, 500)
			}
		case <-time.After(3 * time.Second):
			s.resultBatchesChan <- s.resultsBuffer
			s.resultsBuffer = make([]v1.AnalysisResultV1, 0, 500)
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

func NewV1(migrator migrator.SkeletonMigrator, analysisService AnalysisService, analysisRepository repositories.AnalysisRepository, cerberusClient clients.CerberusV1) v1.SkeletonAPI {
	return &skeleton{
		migrator:           migrator,
		analysisService:    analysisService,
		analysisRepository: analysisRepository,
		cerberusClient:     cerberusClient,
		resultsBuffer:      make([]v1.AnalysisResultV1, 0, 500),
		resultsChan:        make(chan v1.AnalysisResultV1, 500),
		resultBatchesChan:  make(chan []v1.AnalysisResultV1, 10),
	}
}

func NewV1Default(sqlConn *sqlx.DB, dbSchema string, config *config.Configuration) (v1.SkeletonAPI, error) {
	authManager := authmanager.NewAuthManager(config,
		clients.NewRestyClient(context.Background(), config, true))
	authManager.StartClientCredentialTask(context.Background())
	internalApiRestyClient := clients.NewRestyClientWithAuthManager(context.Background(), config, authManager)
	cerberusClient, err := clients.NewCerberusV1Client(config.CerberusURL, internalApiRestyClient)
	analysisService := NewAnalysisService()
	if err != nil {
		return nil, err
	}
	dbConn := db.CreateDbConnector(sqlConn)
	return NewV1(migrator.NewSkeletonMigrator(), analysisService, repositories.NewAnalysisRepository(dbConn, dbSchema), cerberusClient), nil
}
