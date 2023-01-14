package skeleton

import (
	"context"
	"github.com/DRK-Blutspende-BaWueHe/skeleton/migrator"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

type skeleton struct {
	sqlConn            *sqlx.DB
	dbSchema           string
	migrator           migrator.SkeletonMigrator
	api                GinApi
	analysisRepository AnalysisRepository
	instrumentService  InstrumentService
	resultsBuffer      []AnalysisResult
	resultsChan        chan AnalysisResult
	resultBatchesChan  chan []AnalysisResult
	cerberusClient     CerberusV1
	manager            CallbackManager
}

func (s *skeleton) SetCallbackHandler(eventHandler SkeletonCallbackHandlerV1) {
	s.manager.SetCallbackHandler(eventHandler)
}
func (s *skeleton) GetCallbackHandler() SkeletonCallbackHandlerV1 {
	return s.manager.GetCallbackHandler()
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

func (s *skeleton) GetAnalysisRequestWithNoResults(currentPage, itemsPerPage int) (requests []AnalysisRequest, maxPages int, err error) {

	return []AnalysisRequest{}, 0, nil
}

func (s *skeleton) GetAnalysisRequestsBySampleCode(sampleCode string) ([]AnalysisRequest, error) {
	return []AnalysisRequest{}, nil
}

func (s *skeleton) GetAnalysisRequestsBySampleCodes(sampleCodes []string) ([]AnalysisRequest, error) {
	return []AnalysisRequest{}, nil
}

func (s *skeleton) GetRequestMappingsByInstrumentID(instrumentID uuid.UUID) ([]RequestMapping, error) {
	return []RequestMapping{}, nil
}

func (s *skeleton) SubmitAnalysisResult(ctx context.Context, resultData AnalysisResult, submitTypes ...SubmitType) error {
	tx, err := s.analysisRepository.CreateTransaction()
	if err != nil {
		return err
	}
	_, err = s.analysisRepository.WithTransaction(tx).CreateAnalysisResultsBatch(ctx, []AnalysisResult{resultData})
	if err != nil {
		return err
	}

	s.resultsChan <- resultData
	return nil
}

func (s *skeleton) GetInstrument(instrumentID uuid.UUID) (Instrument, error) {
	return s.instrumentService.GetInstrumentByID(context.TODO(), instrumentID)
}

func (s *skeleton) GetInstrumentByIP(ip string) (Instrument, error) {
	return s.instrumentService.GetInstrumentByIP(context.TODO(), ip)
}

func (s *skeleton) GetInstruments() ([]Instrument, error) {
	return s.instrumentService.GetInstruments(context.TODO())
}

func (s *skeleton) FindAnalyteByManufacturerTestCode(instrument Instrument, testCode string) AnalyteMapping {
	return AnalyteMapping{}
}

func (s *skeleton) FindResultMapping(searchValue string, mapping []ResultMapping) (string, error) {
	return "", nil
}

func (s *skeleton) RegisterProtocol(ctx context.Context, id uuid.UUID, name string, description string, abilities []ProtocolAbility) error {
	err := s.instrumentService.UpsertSupportedProtocol(ctx, id, name, description)
	if err != nil {
		return err
	}
	return s.instrumentService.UpsertProtocolAbilities(ctx, id, abilities)
}

func (s *skeleton) SetOnlineStatus(ctx context.Context, id uuid.UUID, status InstrumentStatus) error {
	return s.instrumentService.UpdateInstrumentStatus(ctx, id, status)
}

func (s *skeleton) migrateUp(ctx context.Context, db *sqlx.DB, schemaName string) error {
	return s.migrator.Run(ctx, db, schemaName)
}

func (s *skeleton) Start() error {
	go s.processAnalysisResults(context.Background())
	go s.processAnalysisResultBatches(context.Background())

	// Todo - use cancellable context what is passed to the routines above too
	err := s.api.Run()
	if err != nil {
		log.Error().Err(err).Msg("Failed to start API")
		return err
	}

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
				s.resultsBuffer = make([]AnalysisResult, 0, 500)
			}
		case <-time.After(3 * time.Second):
			s.resultBatchesChan <- s.resultsBuffer
			s.resultsBuffer = make([]AnalysisResult, 0, 500)
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

func NewSkeleton(sqlConn *sqlx.DB, dbSchema string, migrator migrator.SkeletonMigrator, api GinApi, analysisRepository AnalysisRepository, instrumentService InstrumentService, manager CallbackManager, cerberusClient CerberusV1) (SkeletonAPI, error) {
	skeleton := &skeleton{
		sqlConn:            sqlConn,
		dbSchema:           dbSchema,
		migrator:           migrator,
		api:                api,
		analysisRepository: analysisRepository,
		instrumentService:  instrumentService,
		manager:            manager,
		cerberusClient:     cerberusClient,
		resultsBuffer:      make([]AnalysisResult, 0, 500),
		resultsChan:        make(chan AnalysisResult, 500),
		resultBatchesChan:  make(chan []AnalysisResult, 10),
	}

	err := skeleton.migrateUp(context.Background(), skeleton.sqlConn, skeleton.dbSchema)
	if err != nil {
		return nil, err
	}

	// Note: Cache instruments on startup
	go func() {
		_, _ = instrumentService.GetInstruments(context.Background())
	}()

	return skeleton, nil
}
