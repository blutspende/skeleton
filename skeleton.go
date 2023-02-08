package skeleton

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/DRK-Blutspende-BaWueHe/skeleton/consolelog/service"
	"time"

	"github.com/DRK-Blutspende-BaWueHe/skeleton/migrator"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog/log"
)

type skeleton struct {
	sqlConn            *sqlx.DB
	dbSchema           string
	migrator           migrator.SkeletonMigrator
	api                GinApi
	analysisRepository AnalysisRepository
	analysisService    AnalysisService
	instrumentService  InstrumentService
	consoleLogService  service.ConsoleLogService
	resultsBuffer      []AnalysisResult
	resultBatchesChan  chan []AnalysisResult
	cerberusClient     Cerberus
	manager            Manager
}

func (s *skeleton) SetCallbackHandler(eventHandler SkeletonCallbackHandlerV1) {
	s.manager.SetCallbackHandler(eventHandler)
}
func (s *skeleton) GetCallbackHandler() SkeletonCallbackHandlerV1 {
	return s.manager.GetCallbackHandler()
}

func (s *skeleton) Log(instrumentID uuid.UUID, msg string) {
	log.Info().Interface("instrumentId", instrumentID).Msg(msg)
	s.consoleLogService.Info(instrumentID, "[GENERAL]", msg)
}

func (s *skeleton) LogError(instrumentID uuid.UUID, err error) {
	log.Error().Interface("instrumentId", instrumentID).Err(err).Msg("")
	s.consoleLogService.Error(instrumentID, "[GENERAL]", err.Error())
}

func (s *skeleton) LogDebug(instrumentID uuid.UUID, msg string) {
	log.Debug().Interface("instrumentId", instrumentID).Msg(msg)
	s.consoleLogService.Debug(instrumentID, "[GENERAL]", msg)
}

func (s *skeleton) GetAnalysisRequestWithNoResults(currentPage, itemsPerPage int) (requests []AnalysisRequest, maxPages int, err error) {

	return []AnalysisRequest{}, 0, nil
}

func (s *skeleton) GetAnalysisRequestsBySampleCode(sampleCode string) ([]AnalysisRequest, error) {
	analysisRequests, err := s.analysisRepository.GetAnalysisRequestsBySampleCodes(context.TODO(), []string{sampleCode})
	if err != nil {
		return []AnalysisRequest{}, nil
	}

	return analysisRequests[sampleCode], nil
}

func (s *skeleton) GetAnalysisRequestsBySampleCodes(sampleCodes []string) (map[string][]AnalysisRequest, error) {
	analysisRequests, err := s.analysisRepository.GetAnalysisRequestsBySampleCodes(context.TODO(), sampleCodes)
	if err != nil {
		return map[string][]AnalysisRequest{}, nil
	}

	return analysisRequests, nil
}

func (s *skeleton) GetRequestMappingsByInstrumentID(instrumentID uuid.UUID) ([]RequestMapping, error) {
	return []RequestMapping{}, nil
}

func (s *skeleton) SubmitAnalysisResult(ctx context.Context, resultData AnalysisResult, submitTypes ...SubmitType) error {
	if resultData.AnalyteMapping.ID == uuid.Nil {
		return errors.New("analyte mapping ID is missing")
	}
	if resultData.Instrument.ID == uuid.Nil {
		return errors.New("instrument ID is missing")
	}
	if resultData.TechnicalReleaseDateTime.IsZero() {
		return errors.New("technical release date-time is missing")
	}

	if resultData.ResultMode == "" {
		resultData.ResultMode = resultData.Instrument.ResultMode
	}

	tx, err := s.analysisRepository.CreateTransaction()
	if err != nil {
		return err
	}
	savedResultDataList, err := s.analysisRepository.WithTransaction(tx).CreateAnalysisResultsBatch(ctx, []AnalysisResult{resultData})
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	savedResultData := savedResultDataList[0]
	analyteRequests, err := s.analysisRepository.GetAnalysisRequestsBySampleCodeAndAnalyteID(ctx, savedResultData.SampleCode, savedResultData.AnalyteMapping.AnalyteID)
	if err != nil {
		return err
	}
	for i := range analyteRequests {
		savedResultData.AnalysisRequest = analyteRequests[i]
		s.manager.SendResultForProcessing(savedResultData)
	}
	return nil
}

func (s *skeleton) GetInstrument(instrumentID uuid.UUID) (Instrument, error) {
	return s.instrumentService.GetInstrumentByID(context.TODO(), nil, instrumentID, false)
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

func (s *skeleton) FindResultEntities(InstrumentID uuid.UUID, sampleCode string, ManufacturerTestCode string) (Instrument, []AnalysisRequest, AnalyteMapping, error) {
	instrument, err := s.GetInstrument(InstrumentID)
	if err != nil {
		return Instrument{}, []AnalysisRequest{}, AnalyteMapping{}, err
	}

	var analyteMapping AnalyteMapping
	for _, mapping := range instrument.AnalyteMappings {
		if mapping.InstrumentAnalyte == ManufacturerTestCode {
			analyteMapping = mapping
			break
		}
	}

	allAnalysisRequests, err := s.GetAnalysisRequestsBySampleCode(sampleCode) // if there are none, that shouldnt be an error but an empty array
	if err != nil {
		return Instrument{}, []AnalysisRequest{}, AnalyteMapping{}, err
	}

	analysisRequests := make([]AnalysisRequest, 0)
	for _, ar := range allAnalysisRequests {
		if ar.AnalyteID == analyteMapping.AnalyteID {
			analysisRequests = append(analysisRequests, ar)
		}
	}

	return instrument, analysisRequests, analyteMapping, nil
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
	go s.cleanUpCerberusQueueItems(context.Background())
	go s.sendUnsentInstrumentsToCerberus(context.Background())
	go s.processAnalysisRequests(context.Background())
	go s.processAnalysisResults(context.Background())
	go s.processAnalysisResultBatches(context.Background())
	go s.submitAnalysisResultsToCerberus(context.Background())

	// Todo - use cancellable context what is passed to the routines above too
	err := s.api.Run()
	if err != nil {
		log.Error().Err(err).Msg("Failed to start API")
		return err
	}

	return nil
}

func (s *skeleton) sendUnsentInstrumentsToCerberus(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Minute)
	for {
		select {
		case <-ctx.Done():
			ticker.Stop()
			return
		case _, ok := <-ticker.C:
			if !ok {
				log.Error().Msg("Sending unsent instruments to Cerberus stopped")
			}
			s.instrumentService.EnqueueUnsentInstrumentsToCerberus(ctx)
		}
	}
}

func (s *skeleton) processAnalysisRequests(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case requests, ok := <-s.manager.GetProcessableAnalysisRequestsChan():
			if !ok {
				log.Fatal().Msg("processing analysis requests stopped: requests channel closed")
			}
			err := s.GetCallbackHandler().HandleAnalysisRequests(requests)
			if err != nil {
				log.Error().Err(err).Msg("Received veto from analysis result handler, aborting result transmission for the whole batch")
				break
			}
			log.Debug().Msgf("Processing analysis requests in %d batch", len(requests))
			err = s.analysisService.ProcessAnalysisRequests(ctx, requests)
			if err != nil {
				log.Error().Err(err).Msg("Failed to process analysis requests")
				break
			}
		}
	}
}

func (s *skeleton) processAnalysisResults(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case result, ok := <-s.manager.GetResultChan():
			if !ok {
				log.Fatal().Msg("processing analysis results stopped: results channel closed")
			}
			s.resultsBuffer = append(s.resultsBuffer, result)
			if len(s.resultsBuffer) >= 500 {
				s.resultBatchesChan <- s.resultsBuffer
				s.resultsBuffer = make([]AnalysisResult, 0, 500)
			}
		case <-time.After(3 * time.Second):
			if len(s.resultsBuffer) > 0 {
				s.resultBatchesChan <- s.resultsBuffer
				s.resultsBuffer = make([]AnalysisResult, 0, 500)
			}
		}
	}
}

func (s *skeleton) processAnalysisResultBatches(ctx context.Context) {
	for {
		resultsBatch, ok := <-s.resultBatchesChan
		if !ok {
			log.Fatal().Msg("processing analysis result batches stopped: resultBatches channel closed")
		}

		if len(resultsBatch) < 1 {
			continue
		}

		_, err := s.analysisRepository.CreateAnalysisResultQueueItem(ctx, resultsBatch)
		if err != nil {
			time.AfterFunc(30*time.Second, func() {
				s.resultBatchesChan <- resultsBatch
			})
			continue
		}
	}
}

func (s *skeleton) cleanUpCerberusQueueItems(ctx context.Context) {
	// Todo
	//-- clean up old stuff
	//delete from astm.sk_cerberus_queue_items where created_at < now()-interval '14 days';
}

func (s *skeleton) submitAnalysisResultsToCerberus(ctx context.Context) {
	ticker := time.NewTicker(60 * time.Second)
	for {
		select {
		case <-ctx.Done():
			ticker.Stop()
			return
		case <-ticker.C:
			queueItems, err := s.analysisRepository.GetAnalysisResultQueueItems(ctx)
			if err != nil {
				log.Error().Err(err).Msg("Failed to get cerberus queue items")
			}
			for _, queueItem := range queueItems {
				var analysisResult []AnalysisResult
				if err = json.Unmarshal([]byte(queueItem.JsonMessage), &analysisResult); err != nil {
					log.Error().Err(err).Msg("Failed to unmarshal analysis results")
					continue
				}

				response, err := s.cerberusClient.SendAnalysisResultBatch(analysisResult)
				if err != nil {
					log.Error().Err(err).Msg("Failed to send analysis result to cerberus")
				}

				if !response.HasResult() {
					continue
				}

				responseJsonMessage, _ := json.Marshal(response.AnalysisResultBatchItemInfoList)

				cerberusQueueItem := CerberusQueueItem{
					ID:                  queueItem.ID,
					LastHTTPStatus:      response.HTTPStatusCode,
					LastError:           response.ErrorMessage,
					RawResponse:         response.RawResponse,
					ResponseJsonMessage: string(responseJsonMessage),
				}
				if !response.IsSuccess() {
					utcNow := time.Now().UTC()
					cerberusQueueItem.LastErrorAt = &utcNow
					cerberusQueueItem.RetryNotBefore = utcNow.Add(10 * time.Minute)
				}

				err = s.analysisRepository.UpdateAnalysisResultQueueItemStatus(ctx, cerberusQueueItem)
				if err != nil {
					log.Error().Err(err).Msg("Failed to update the status of the cerberus queue item")
				}
			}
		}
	}
}

func NewSkeleton(sqlConn *sqlx.DB, dbSchema string, migrator migrator.SkeletonMigrator, api GinApi, analysisRepository AnalysisRepository, analysisService AnalysisService, instrumentService InstrumentService, consoleLogService service.ConsoleLogService, manager Manager, cerberusClient Cerberus) (SkeletonAPI, error) {
	skeleton := &skeleton{
		sqlConn:            sqlConn,
		dbSchema:           dbSchema,
		migrator:           migrator,
		api:                api,
		analysisRepository: analysisRepository,
		analysisService:    analysisService,
		instrumentService:  instrumentService,
		consoleLogService:  consoleLogService,
		manager:            manager,
		cerberusClient:     cerberusClient,
		resultsBuffer:      make([]AnalysisResult, 0, 500),
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
