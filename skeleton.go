package skeleton

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/blutspende/skeleton/config"
	"github.com/blutspende/skeleton/utils"

	"github.com/blutspende/skeleton/consolelog/service"

	"github.com/blutspende/skeleton/migrator"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog/log"
)

type skeleton struct {
	ctx                        context.Context
	config                     config.Configuration
	sqlConn                    *sqlx.DB
	dbSchema                   string
	migrator                   migrator.SkeletonMigrator
	api                        GinApi
	analysisRepository         AnalysisRepository
	analysisService            AnalysisService
	instrumentService          InstrumentService
	consoleLogService          service.ConsoleLogService
	sortingRuleService         SortingRuleService
	resultsBuffer              []AnalysisResult
	resultBatchesChan          chan []AnalysisResult
	cerberusClient             CerberusClient
	longPollClient             LongPollClient
	deaClient                  DeaClientV1
	manager                    Manager
	resultTransferFlushTimeout int
	imageRetrySeconds          int
	serviceName                string
	displayName                string
	extraValueKeys             []string
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

func (s *skeleton) GetAnalysisRequestsBySampleCode(ctx context.Context, sampleCode string, allowResending bool) ([]AnalysisRequest, error) {
	analysisRequests, err := s.analysisRepository.GetAnalysisRequestsBySampleCodes(ctx, []string{sampleCode}, allowResending)
	if err != nil {
		return []AnalysisRequest{}, err
	}

	return analysisRequests[sampleCode], nil
}

func (s *skeleton) GetAnalysisRequestsBySampleCodes(ctx context.Context, sampleCodes []string, allowResending bool) (map[string][]AnalysisRequest, error) {
	analysisRequests, err := s.analysisRepository.GetAnalysisRequestsBySampleCodes(ctx, sampleCodes, allowResending)
	if err != nil {
		return map[string][]AnalysisRequest{}, err
	}

	return analysisRequests, nil
}

func (s *skeleton) GetAnalysisRequestExtraValues(ctx context.Context, analysisRequestID uuid.UUID) (map[string]string, error) {
	return s.analysisRepository.GetAnalysisRequestExtraValuesByAnalysisRequestID(ctx, analysisRequestID)
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

func (s *skeleton) SubmitAnalysisResult(ctx context.Context, resultData AnalysisResult) error {
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
		_ = tx.Rollback()
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

func (s *skeleton) SubmitAnalysisResultBatch(ctx context.Context, resultBatch []AnalysisResult) error {
	for i := range resultBatch {
		if resultBatch[i].AnalyteMapping.ID == uuid.Nil {
			return errors.New(fmt.Sprintf("analyte mapping ID is missing at index: %d", i))
		}
		if resultBatch[i].Instrument.ID == uuid.Nil {
			return errors.New(fmt.Sprintf("instrument ID is missing at index: %d", i))
		}
		if resultBatch[i].ResultMode == "" {
			resultBatch[i].ResultMode = resultBatch[i].Instrument.ResultMode
		}
	}

	tx, err := s.analysisRepository.CreateTransaction()
	if err != nil {
		return err
	}
	savedAnalysisResults, err := s.analysisRepository.WithTransaction(tx).CreateAnalysisResultsBatch(ctx, resultBatch)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	if err = tx.Commit(); err != nil {
		_ = tx.Rollback()
		return err
	}

	go func(analysisResults []AnalysisResult) {
		for i := range analysisResults {
			err := s.saveImages(ctx, &analysisResults[i])
			if err != nil {
				log.Error().Err(err).Str("analysisResultID", analysisResults[i].ID.String()).Msg("save images of analysis result failed")
			}
			analyteRequests, err := s.analysisRepository.GetAnalysisRequestsBySampleCodeAndAnalyteID(ctx, analysisResults[i].SampleCode, analysisResults[i].AnalyteMapping.AnalyteID)
			if err != nil {
				log.Error().Err(err).Msg("get analysis requests by sample code and analyteID failed after saving results")
				return
			}

			for j := range analyteRequests {
				analysisResults[i].AnalysisRequest = analyteRequests[j]
				s.manager.SendResultForProcessing(analysisResults[i])
			}
		}
	}(savedAnalysisResults)

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

func (s *skeleton) GetSortingTarget(ctx context.Context, instrumentIP string, sampleCode string, programme string) (string, error) {
	instrument, err := s.GetInstrumentByIP(ctx, instrumentIP)
	if err != nil {
		log.Error().Err(err).Str("instrumentIP", instrumentIP).Str("sampleCode", sampleCode).Msg("failed to get sorting target")
		return "", err
	}

	sortingRules, err := s.sortingRuleService.GetByInstrumentIDAndProgramme(ctx, instrument.ID, programme)
	if err != nil {
		log.Error().Err(err).Str("instrumentIP", instrumentIP).Str("sampleCode", sampleCode).Msg("failed to get sorting target")
		return "", err
	}

	analysisRequests, err := s.GetAnalysisRequestsBySampleCode(ctx, sampleCode, true)
	if len(analysisRequests) == 0 {
		analysisRequests = []AnalysisRequest{
			{SampleCode: sampleCode},
		}
	} else {
		extraValuesMap, err := s.GetAnalysisRequestExtraValues(ctx, analysisRequests[0].ID)
		if err != nil {
			log.Error().Err(err).Send()
		}
		for i := range analysisRequests {
			for key, value := range extraValuesMap {
				analysisRequests[i].ExtraValues = append(analysisRequests[i].ExtraValues, ExtraValue{
					Key:   key,
					Value: value,
				})
			}
		}
	}
	var appliedTargets []string
	sampleSequenceNumber := 1
	targetsLoaded := false
	sampleSequenceNumberLoaded := false
	for i := range sortingRules {
		if !targetsLoaded && ConditionHasOperator(sortingRules[i].Condition, TargetApplied, TargetNotApplied) {
			appliedTargets, err = s.sortingRuleService.GetAppliedSortingRuleTargets(ctx, instrument.ID, programme, analysisRequests[0])
			if err == nil {
				targetsLoaded = true
			}
		}

		if !sampleSequenceNumberLoaded && ConditionHasOperator(sortingRules[i].Condition, IsNthSample) {
			sampleSequenceNumber, err = s.sortingRuleService.GetSampleSequenceNumber(ctx, sampleCode)
			if err == nil {
				sampleSequenceNumberLoaded = true
			}
		}
		target, err := GetSortingTargetForAnalysisRequestAndCondition(analysisRequests, sortingRules[i], appliedTargets, sampleSequenceNumber)
		if err != nil {
			continue
		}
		return target, nil
	}

	return "", fmt.Errorf("no target found")
}

func (s *skeleton) MarkSortingTargetAsApplied(ctx context.Context, instrumentIP, sampleCode, programme, target string) error {
	instrument, err := s.GetInstrumentByIP(ctx, instrumentIP)
	if err != nil {
		return err
	}
	analysisRequestsBySampleCode, err := s.GetAnalysisRequestsBySampleCodes(ctx, []string{sampleCode}, true)
	if err != nil {
		return err
	}
	var validUntil time.Time
	analysisRequests := analysisRequestsBySampleCode[sampleCode]
	if len(analysisRequests) == 0 {
		validUntil = time.Now().UTC().AddDate(0, 3, 0)
	} else {
		validUntil = analysisRequests[0].ValidUntilTime
	}
	return s.sortingRuleService.ApplySortingRuleTarget(ctx, instrument.ID, programme, sampleCode, target, validUntil)
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

func (s *skeleton) RegisterManufacturerTests(ctx context.Context, manufacturerTests []SupportedManufacturerTests) error {
	return s.instrumentService.UpsertManufacturerTests(ctx, manufacturerTests)
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

const apiVersion = "v1"

func (s *skeleton) Start() error {
	err := s.registerDriverToCerberus(s.ctx)
	if err != nil {
		log.Error().Err(err).Msg("starting skeleton failed due to failed registration of instrument driver to cerberus")
		return err
	}
	go func() {
		s.runCleanupJobs()
		for {
			select {
			case <-time.After(time.Hour * time.Duration(s.config.CleanupJobRunIntervalHours)):
				s.runCleanupJobs()
			case <-s.ctx.Done():
				return
			}
		}
	}()
	for i := 0; i < s.config.AnalysisRequestWorkerPoolSize; i++ {
		go s.processAnalysisRequests(s.ctx)
	}
	go s.processAnalysisResults(s.ctx)
	go s.processAnalysisResultBatches(s.ctx)
	go s.submitAnalysisResultsToCerberus(s.ctx)
	go s.processStuckImagesToDEA(s.ctx)
	go s.processStuckImagesToCerberus(s.ctx)
	go s.enqueueUnprocessedAnalysisRequests(s.ctx)
	go s.enqueueUnprocessedAnalysisResults(s.ctx)
	go s.longPollClient.StartLongPoll(s.ctx)

	// Todo - use cancellable context what is passed to the routines above too
	err = s.api.Run()
	if err != nil {
		log.Error().Err(err).Msg("Failed to start API")
		return err
	}

	return nil
}

func (s *skeleton) registerDriverToCerberus(ctx context.Context) error {
	retryCount := 0
	for {
		protocols, err := s.instrumentService.GetSupportedProtocols(ctx)
		if err != nil {
			log.Error().Err(err).Msg("failed to get supported protocols")
			return err
		}
		protocolsTos := convertSupportedProtocolsToSupportedProtocolTOs(protocols)

		manufacturerTests, err := s.instrumentService.GetManufacturerTests(ctx)
		if err != nil {
			log.Error().Err(err).Msg("failed to get manufacturer tests")
			return err
		}
		manufacturerTestTOs := convertSupportedManufacturerTestsToSupportedManufacturerTestTOs(manufacturerTests)

		err = s.cerberusClient.RegisterInstrumentDriver(s.serviceName, s.displayName, apiVersion, s.config.APIPort, s.config.EnableTLS, s.extraValueKeys, protocolsTos, manufacturerTestTOs)
		if err != nil {
			log.Warn().Err(err).Int("retryCount", retryCount).Msg("register instrument driver to cerberus failed")
			retryCount++
			if retryCount >= s.config.InstrumentDriverRegistrationMaxRetry {
				break
			}
			time.Sleep(time.Duration(s.config.InstrumentDriverRegistrationTimeoutSeconds) * time.Second)
			continue
		}
		return nil
	}
	return errors.New("register instrument driver to cerberus failed too many times")
}

func (s *skeleton) enqueueUnprocessedAnalysisRequests(ctx context.Context) {
	for {
		requests, err := s.analysisRepository.GetUnprocessedAnalysisRequests(ctx)
		if err != nil {
			time.Sleep(5 * time.Minute)
			continue
		}

		for {
			failed := make([]AnalysisRequest, 0)

			err = utils.Partition(len(requests), 500, func(low int, high int) error {
				partition := requests[low:high]

				requestIDs := make([]uuid.UUID, 0)

				for _, request := range partition {
					requestIDs = append(requestIDs, request.ID)
				}

				subjects, err := s.analysisRepository.GetSubjectsByAnalysisRequestIDs(ctx, requestIDs)
				if err != nil {
					failed = append(failed, partition...)
					return err
				}

				for i := range partition {
					if subject, ok := subjects[partition[i].ID]; ok {
						partition[i].SubjectInfo = &subject
					}
				}

				s.manager.GetProcessableAnalysisRequestQueue().Enqueue(partition)

				return nil
			})

			if err != nil {
				requests = failed
				time.Sleep(5 * time.Minute)
				continue
			}

			break
		}

		break
	}
}

func (s *skeleton) enqueueUnprocessedAnalysisResults(ctx context.Context) {
	for {
		resultIDs, err := s.analysisRepository.GetUnprocessedAnalysisResultIDs(ctx)
		if err != nil {
			time.Sleep(5 * time.Minute)
			continue
		}

		for {
			err = utils.Partition(len(resultIDs), 500, func(low int, high int) error {
				results, err := s.analysisRepository.GetAnalysisResultsByIDs(ctx, resultIDs[low:high])
				if err != nil {
					return err
				}

				for _, result := range results {
					s.manager.SendResultForProcessing(result)
				}

				return nil
			})
			if err != nil {
				log.Error().Err(err).Msg("GetAnalysisResultsByIDs failed")
			}

			break
		}

		break
	}
}

func (s *skeleton) processAnalysisRequests(ctx context.Context) {
	log.Trace().Msg("Starting to process analysis requests")

	for {
		select {
		case <-ctx.Done():
			log.Trace().Msg("Stopping to process analysis requests")
			return
		default:
			requests := s.manager.GetProcessableAnalysisRequestQueue().Dequeue()

			if ctx.Err() != nil {
				log.Trace().Msg("Stopping to process analysis requests")
				return
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

		err := s.analysisService.QueueAnalysisResults(ctx, resultsBatch)
		if err != nil {
			time.AfterFunc(30*time.Second, func() {
				s.resultBatchesChan <- resultsBatch
			})
			continue
		}
	}
}

const limit = 5000

func (s *skeleton) runCleanupJobs() {
	s.cleanupCerberusQueueItems()
	s.cleanupAnalysisResults()
	s.cleanupAnalysisRequests()
}

func (s *skeleton) cleanupCerberusQueueItems() {
	for {
		deletedRows, err := s.analysisRepository.DeleteOldCerberusQueueItems(s.ctx, s.config.CleanupDays, limit)
		if err != nil {
			log.Error().Err(err).Msg("cleanup old cerberus queue items failed")
			return
		}
		if int(deletedRows) < limit {
			return
		}
	}
}

func (s *skeleton) cleanupAnalysisRequests() {
	for {
		tx, err := s.analysisRepository.CreateTransaction()
		if err != nil {
			log.Error().Err(err).Msg("cleanup old analysis requests failed")
			return
		}
		deletedRows, err := s.analysisRepository.DeleteOldAnalysisRequestsWithTx(s.ctx, s.config.CleanupDays, limit, tx)
		if err != nil {
			_ = tx.Rollback()
			log.Error().Err(err).Msg("cleanup old analysis requests failed")
			return
		}
		err = tx.Commit()
		if err != nil {
			_ = tx.Rollback()
			log.Error().Err(err).Msg("cleanup old analysis requests failed")
			return
		}
		if int(deletedRows) < limit {
			return
		}
	}
}

func (s *skeleton) cleanupAnalysisResults() {
	for {
		tx, err := s.analysisRepository.CreateTransaction()
		if err != nil {
			log.Error().Err(err).Msg("cleanup old analysis results failed")
			return
		}
		deletedRows, err := s.analysisRepository.DeleteOldAnalysisResultsWithTx(s.ctx, s.config.CleanupDays, limit, tx)
		if err != nil {
			_ = tx.Rollback()
			log.Error().Err(err).Msg("cleanup old analysis results failed")
			return
		}
		err = tx.Commit()
		if err != nil {
			_ = tx.Rollback()
			log.Error().Err(err).Msg("cleanup old analysis results failed")
			return
		}
		if int(deletedRows) < limit {
			return
		}
	}
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
			if len(queueItems) == 0 {
				continue
			}
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

func NewSkeleton(ctx context.Context, serviceName, displayName string, requestedExtraValueKeys []string, sqlConn *sqlx.DB, dbSchema string, migrator migrator.SkeletonMigrator, api GinApi, analysisRepository AnalysisRepository, analysisService AnalysisService, instrumentService InstrumentService, consoleLogService service.ConsoleLogService, sortingRuleService SortingRuleService, manager Manager, cerberusClient CerberusClient, longpollClient LongPollClient, deaClient DeaClientV1, config config.Configuration) (SkeletonAPI, error) {
	skeleton := &skeleton{
		ctx:                        ctx,
		serviceName:                serviceName,
		displayName:                displayName,
		extraValueKeys:             requestedExtraValueKeys,
		config:                     config,
		sqlConn:                    sqlConn,
		dbSchema:                   dbSchema,
		migrator:                   migrator,
		api:                        api,
		analysisRepository:         analysisRepository,
		analysisService:            analysisService,
		instrumentService:          instrumentService,
		consoleLogService:          consoleLogService,
		sortingRuleService:         sortingRuleService,
		manager:                    manager,
		cerberusClient:             cerberusClient,
		longPollClient:             longpollClient,
		deaClient:                  deaClient,
		resultsBuffer:              make([]AnalysisResult, 0, 500),
		resultBatchesChan:          make(chan []AnalysisResult, 10),
		resultTransferFlushTimeout: config.ResultTransferFlushTimeout,
		imageRetrySeconds:          config.ImageRetrySeconds,
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
