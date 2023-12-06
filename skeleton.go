package skeleton

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/DRK-Blutspende-BaWueHe/skeleton/consolelog/service"

	"github.com/DRK-Blutspende-BaWueHe/skeleton/migrator"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog/log"
)

type skeleton struct {
	sqlConn                    *sqlx.DB
	dbSchema                   string
	migrator                   migrator.SkeletonMigrator
	api                        GinApi
	analysisRepository         AnalysisRepository
	analysisService            AnalysisService
	instrumentService          InstrumentService
	consoleLogService          service.ConsoleLogService
	resultsBuffer              []AnalysisResult
	resultBatchesChan          chan []AnalysisResult
	cerberusClient             Cerberus
	deaClient                  DeaClientV1
	manager                    Manager
	resultTransferFlushTimeout int
	imageRetrySeconds          int
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

func (s *skeleton) GetAnalysisRequestWithNoResults(ctx context.Context, currentPage, itemsPerPage int) (requests []AnalysisRequest, maxPages int, err error) {

	return []AnalysisRequest{}, 0, nil
}

func (s *skeleton) GetAnalysisRequestsBySampleCode(ctx context.Context, sampleCode string, allowResending bool) ([]AnalysisRequest, error) {
	analysisRequests, err := s.analysisRepository.GetAnalysisRequestsBySampleCodes(ctx, []string{sampleCode}, allowResending)
	if err != nil {
		return []AnalysisRequest{}, nil
	}

	return analysisRequests[sampleCode], nil
}

func (s *skeleton) GetAnalysisRequestsBySampleCodes(ctx context.Context, sampleCodes []string, allowResending bool) (map[string][]AnalysisRequest, error) {
	analysisRequests, err := s.analysisRepository.GetAnalysisRequestsBySampleCodes(ctx, sampleCodes, allowResending)
	if err != nil {
		return map[string][]AnalysisRequest{}, nil
	}

	return analysisRequests, nil
}

func (s *skeleton) SaveAnalysisRequestsInstrumentTransmissions(ctx context.Context, analysisRequestIDs []uuid.UUID, instrumentID uuid.UUID) error {
	tx, err := s.analysisRepository.CreateTransaction()
	if err != nil {
		return err
	}
	err = s.analysisRepository.WithTransaction(tx).IncreaseSentToInstrumentCounter(ctx, analysisRequestIDs)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	err = s.analysisRepository.WithTransaction(tx).SaveAnalysisRequestsInstrumentTransmissions(ctx, analysisRequestIDs, instrumentID)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	err = tx.Commit()
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	return nil
}

func (s *skeleton) GetRequestMappingsByInstrumentID(ctx context.Context, instrumentID uuid.UUID) ([]RequestMapping, error) {
	return []RequestMapping{}, nil
}

func (s *skeleton) SubmitAnalysisResult(ctx context.Context, resultData AnalysisResult, submitTypes ...SubmitType) error {
	if resultData.AnalyteMapping.ID == uuid.Nil {
		return errors.New("analyte mapping ID is missing")
	}
	if resultData.Instrument.ID == uuid.Nil {
		return errors.New("instrument ID is missing")
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

	go func(analysisResult AnalysisResult) {
		err := s.saveImages(ctx, &analysisResult)
		if err != nil {
			log.Error().Err(err).Str("analysisResultID", analysisResult.ID.String()).Msg("save images of analysis result failed")
		}
		analyteRequests, err := s.analysisRepository.GetAnalysisRequestsBySampleCodeAndAnalyteID(ctx, analysisResult.SampleCode, analysisResult.AnalyteMapping.AnalyteID)
		if err != nil {
			log.Error().Err(err).Msg("get analysis requests by sample code and analyteID failed after saving results")
			return
		}

		for i := range analyteRequests {
			savedResultData.AnalysisRequest = analyteRequests[i]
			s.manager.SendResultForProcessing(savedResultData)
		}
	}(savedResultData)

	return nil
}

func (s *skeleton) saveImages(ctx context.Context, resultData *AnalysisResult) error {
	if resultData == nil {
		return nil
	}
	imagePtrs := make([]*Image, 0)
	imageDaos := make([]imageDAO, 0)
	for i := range resultData.Images {
		imageDao := imageDAO{
			AnalysisResultID: resultData.ID,
			Name:             resultData.Images[i].Name,
		}
		if resultData.Images[i].Description != nil && len(*resultData.Images[i].Description) > 0 {
			imageDao.Description = sql.NullString{
				String: *resultData.Images[i].Description,
				Valid:  true,
			}
		}
		fileName := resultData.Images[i].Name
		if !strings.HasSuffix(resultData.Images[i].Name, ".jpg") && !strings.HasSuffix(resultData.Images[i].Name, ".png") {
			fileName += ".jpg"
		}
		id, err := s.deaClient.UploadImage(resultData.Images[i].ImageBytes, fileName)
		if err != nil {
			imageDao.ImageBytes = resultData.Images[i].ImageBytes
			imageDao.UploadError = sql.NullString{
				String: err.Error(),
				Valid:  true,
			}
			log.Error().Err(err).Str("analysisResultID", resultData.ID.String()).Msg("upload image to dea failed")
		} else {
			imageDao.DeaImageID = uuid.NullUUID{
				UUID:  id,
				Valid: true,
			}
			imageDao.UploadedToDeaAt = sql.NullTime{
				Time:  time.Now().UTC(),
				Valid: true,
			}
		}
		imagePtrs = append(imagePtrs, &resultData.Images[i])
		imageDaos = append(imageDaos, imageDao)
	}
	for i := range resultData.ChannelResults {
		for j := range resultData.ChannelResults[i].Images {
			imageDao := imageDAO{
				AnalysisResultID: resultData.ID,
				ChannelResultID: uuid.NullUUID{
					UUID:  resultData.ChannelResults[i].ID,
					Valid: true,
				},
				Name: resultData.ChannelResults[i].Images[j].Name,
			}
			if resultData.ChannelResults[i].Images[j].Description != nil && len(*resultData.ChannelResults[i].Images[j].Description) > 0 {
				imageDao.Description = sql.NullString{
					String: *resultData.ChannelResults[i].Images[j].Description,
					Valid:  true,
				}
			}

			filename := fmt.Sprintf("%s_chres_%d_%d.jpg", resultData.ID.String(), i, j)
			id, err := s.deaClient.UploadImage(resultData.ChannelResults[i].Images[j].ImageBytes, filename)
			if err != nil {
				imageDao.ImageBytes = resultData.ChannelResults[i].Images[j].ImageBytes
				imageDao.UploadError = sql.NullString{
					String: err.Error(),
					Valid:  true,
				}
				log.Error().
					Err(err).
					Str("analysisResultID", resultData.ID.String()).
					Str("channelResultID", resultData.ChannelResults[i].ID.String()).
					Msg("upload image to dea failed")
			} else {
				imageDao.DeaImageID = uuid.NullUUID{
					UUID:  id,
					Valid: true,
				}
				imageDao.UploadedToDeaAt = sql.NullTime{
					Time:  time.Now().UTC(),
					Valid: true,
				}
			}
			imagePtrs = append(imagePtrs, &resultData.ChannelResults[i].Images[j])
			imageDaos = append(imageDaos, imageDao)
		}
	}
	ids, err := s.analysisRepository.SaveImages(ctx, imageDaos)
	if err != nil {
		return err
	}
	for i := range ids {
		imagePtrs[i].ID = ids[i]
		imagePtrs[i].DeaImageID = imageDaos[i].DeaImageID
	}
	return nil
}

func (s *skeleton) GetInstrument(ctx context.Context, instrumentID uuid.UUID) (Instrument, error) {
	return s.instrumentService.GetInstrumentByID(ctx, nil, instrumentID, false)
}

func (s *skeleton) GetInstrumentByIP(ctx context.Context, ip string) (Instrument, error) {
	return s.instrumentService.GetInstrumentByIP(ctx, ip)
}

func (s *skeleton) GetInstruments(ctx context.Context) ([]Instrument, error) {
	return s.instrumentService.GetInstruments(ctx)
}

func (s *skeleton) FindAnalyteByManufacturerTestCode(instrument Instrument, testCode string) AnalyteMapping {
	return AnalyteMapping{}
}

func (s *skeleton) FindResultMapping(searchValue string, mapping []ResultMapping) (string, error) {
	return "", nil
}

func (s *skeleton) FindResultEntities(ctx context.Context, InstrumentID uuid.UUID, sampleCode string, ManufacturerTestCode string) (Instrument, []AnalysisRequest, AnalyteMapping, error) {
	instrument, err := s.GetInstrument(ctx, InstrumentID)
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

	allAnalysisRequests, err := s.GetAnalysisRequestsBySampleCode(ctx, sampleCode, true) // if there are none, that shouldnt be an error but an empty array
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

func (s *skeleton) RegisterProtocol(ctx context.Context, id uuid.UUID, name string, description string, abilities []ProtocolAbility, settings []ProtocolSetting) error {
	return s.instrumentService.UpsertSupportedProtocol(ctx, id, name, description, abilities, settings)
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
	go s.processStuckImagesToDEA(context.Background())
	go s.processStuckImagesToCerberus(context.Background())

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
			log.Debug().Msgf("Processing %d analysis requests in batch", len(requests))
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
	tickerTriggerDuration := time.Duration(s.resultTransferFlushTimeout) * time.Second
	ticker := time.NewTicker(tickerTriggerDuration)
	continuousTrigger := make(chan []CerberusQueueItem)
	for {
		select {
		case <-ctx.Done():
			log.Debug().Msg("Stopping analysis result submit job")
			ticker.Stop()
			return
		case queueItems := <-continuousTrigger:
			executionStarted := time.Now()
			log.Trace().Msgf("Triggered result sending to cerberus. Sending %d batches", len(queueItems))
			ticker.Stop()

			sentResultCount := 0
			for _, queueItem := range queueItems {
				var analysisResult []AnalysisResultTO
				if err := json.Unmarshal([]byte(queueItem.JsonMessage), &analysisResult); err != nil {
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

				sentResultCount += len(analysisResult)

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

			log.Trace().Int64("elapsedExecutionTime", time.Since(executionStarted).Milliseconds()).
				Msgf("Sent (or tried to send) %d results to cerberus", sentResultCount)

			queueItems, err := s.analysisRepository.GetAnalysisResultQueueItems(ctx)
			if err != nil {
				log.Error().Err(err).Msg("Failed to get cerberus queue items")
			}
			if len(queueItems) < 1 {
				ticker.Reset(tickerTriggerDuration)
				break
			}
			go func() {
				time.Sleep(1 * time.Second)
				continuousTrigger <- queueItems
			}()
		case <-ticker.C:
			log.Trace().Msg("Scheduled result sending to cerberus")
			queueItems, err := s.analysisRepository.GetAnalysisResultQueueItems(ctx)
			if err != nil {
				log.Error().Err(err).Msg("Failed to get cerberus queue items")
				break
			}
			go func() {
				time.Sleep(50 * time.Millisecond)
				continuousTrigger <- queueItems
			}()
		}
	}
}

func (s *skeleton) processStuckImagesToDEA(ctx context.Context) {
	tickerTriggerDuration := time.Duration(s.imageRetrySeconds) * time.Second
	ticker := time.NewTicker(tickerTriggerDuration)

	for {
		select {
		case <-ctx.Done():
			log.Debug().Msg("Stopping DEA stuck image processing job")
			ticker.Stop()
			return
		case <-ticker.C:
			ticker.Stop()
			log.Trace().Msg("Scheduled DEA stuck image processing")
			s.analysisService.ProcessStuckImagesToDEA(ctx)
			ticker.Reset(tickerTriggerDuration)
		}
	}
}

func (s *skeleton) processStuckImagesToCerberus(ctx context.Context) {
	tickerTriggerDuration := time.Duration(s.imageRetrySeconds) * time.Second
	ticker := time.NewTicker(tickerTriggerDuration)

	for {
		select {
		case <-ctx.Done():
			log.Debug().Msg("Stopping cerberus stuck image processing job")
			ticker.Stop()
			return
		case <-ticker.C:
			ticker.Stop()
			log.Trace().Msg("Scheduled cerberus stuck image processing")
			s.analysisService.ProcessStuckImagesToCerberus(ctx)
			ticker.Reset(tickerTriggerDuration)
		}
	}
}

func NewSkeleton(sqlConn *sqlx.DB, dbSchema string, migrator migrator.SkeletonMigrator, api GinApi, analysisRepository AnalysisRepository, analysisService AnalysisService, instrumentService InstrumentService, consoleLogService service.ConsoleLogService, manager Manager, cerberusClient Cerberus, deaClient DeaClientV1, resultTransferFlushTimeout int, imageRetrySeconds int) (SkeletonAPI, error) {
	skeleton := &skeleton{
		sqlConn:                    sqlConn,
		dbSchema:                   dbSchema,
		migrator:                   migrator,
		api:                        api,
		analysisRepository:         analysisRepository,
		analysisService:            analysisService,
		instrumentService:          instrumentService,
		consoleLogService:          consoleLogService,
		manager:                    manager,
		cerberusClient:             cerberusClient,
		deaClient:                  deaClient,
		resultsBuffer:              make([]AnalysisResult, 0, 500),
		resultBatchesChan:          make(chan []AnalysisResult, 10),
		resultTransferFlushTimeout: resultTransferFlushTimeout,
		imageRetrySeconds:          imageRetrySeconds,
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
