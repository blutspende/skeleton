package skeleton

import (
	"context"
	"errors"
	"github.com/blutspende/skeleton/utils"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"time"
)

var ErrAnalysisRequestWithMatchingWorkItemIdFound = errors.New("analysis request with matching workitem id found")

type AnalysisService interface {
	CreateAnalysisRequests(ctx context.Context, analysisRequests []AnalysisRequest) ([]AnalysisRequestStatus, error)
	ProcessAnalysisRequests(ctx context.Context, analysisRequests []AnalysisRequest) error
	RevokeAnalysisRequests(ctx context.Context, workItemIDs []uuid.UUID) error
	ReexamineAnalysisRequestsBatch(ctx context.Context, workItemIDs []uuid.UUID) error
	GetAnalysisRequestsInfo(ctx context.Context, instrumentID uuid.UUID, filter Filter) ([]AnalysisRequestInfo, int, error)
	GetAnalysisResultsInfo(ctx context.Context, instrumentID uuid.UUID, filter Filter) ([]AnalysisResultInfo, int, error)
	GetAnalysisBatches(ctx context.Context, instrumentID uuid.UUID, filter Filter) ([]AnalysisBatch, int, error)
	QueueAnalysisResults(ctx context.Context, results []AnalysisResult) error
	QueueControlResults(ctx context.Context, results []MappedStandaloneControlResult) error
	RetransmitResult(ctx context.Context, resultID uuid.UUID) error
	RetransmitResultBatches(ctx context.Context, batchIDs []uuid.UUID) error
	ProcessStuckImagesToDEA(ctx context.Context)
	ProcessStuckImagesToCerberus(ctx context.Context)
	SaveCerberusIDsForControlResultBatchItems(ctx context.Context, controlResults []ControlResultBatchItemInfo)
	SaveCerberusIDsForAnalysisResultBatchItems(ctx context.Context, analysisResults []AnalysisResultBatchItemInfo)
	GetUnprocessedMappedStandaloneControlResultsByIDs(ctx context.Context, controlResultIDs []uuid.UUID) ([]MappedStandaloneControlResult, error)
}

type analysisService struct {
	analysisRepository AnalysisRepository
	deaClient          DeaClientV1
	cerberusClient     CerberusClient
	manager            Manager
}

func NewAnalysisService(analysisRepository AnalysisRepository, deaClient DeaClientV1, cerberusClient CerberusClient, manager Manager) AnalysisService {
	return &analysisService{
		analysisRepository: analysisRepository,
		deaClient:          deaClient,
		cerberusClient:     cerberusClient,
		manager:            manager,
	}
}

func (as *analysisService) CreateAnalysisRequests(ctx context.Context, analysisRequests []AnalysisRequest) ([]AnalysisRequestStatus, error) {
	ts := time.Now().UTC()
	for i := range analysisRequests {
		analysisRequests[i].CreatedAt = ts
	}
	tx, err := as.analysisRepository.CreateTransaction()
	if err != nil {
		return nil, err
	}
	_, savedAnalysisRequestWorkItemIDs, err := as.analysisRepository.WithTransaction(tx).CreateAnalysisRequestsBatch(ctx, analysisRequests)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}

	savedWorkItemIDsMap := ConvertUUIDsToMap(savedAnalysisRequestWorkItemIDs)
	extraValuesMap := make(map[uuid.UUID][]ExtraValue)
	analysisRequestsToProcess := make([]AnalysisRequest, 0)
	analysisRequestStatuses := make([]AnalysisRequestStatus, len(analysisRequests))
	for i := range analysisRequests {
		analysisRequestStatuses[i] = AnalysisRequestStatus{
			WorkItemID: analysisRequests[i].WorkItemID,
			Error:      nil,
		}
		if _, ok := savedWorkItemIDsMap[analysisRequests[i].WorkItemID]; !ok {
			err = ErrAnalysisRequestWithMatchingWorkItemIdFound
			analysisRequestStatuses[i].Error = err
			continue
		}

		extraValuesMap[analysisRequests[i].ID] = analysisRequests[i].ExtraValues
		analysisRequestsToProcess = append(analysisRequestsToProcess, analysisRequests[i])
	}

	extraValuesErr := as.analysisRepository.WithTransaction(tx).CreateAnalysisRequestExtraValues(ctx, extraValuesMap)
	if extraValuesErr != nil {
		_ = tx.Rollback()
		return nil, extraValuesErr
	}

	commitErr := tx.Commit()
	if commitErr != nil {
		_ = tx.Rollback()
		return nil, commitErr
	}

	as.manager.SendAnalysisRequestsForProcessing(analysisRequestsToProcess)
	return analysisRequestStatuses, err
}

func (as *analysisService) ProcessAnalysisRequests(ctx context.Context, analysisRequests []AnalysisRequest) error {
	completableRequestCount := 0
	processedAnalysisRequestIDs := make([]uuid.UUID, 0)

	for _, request := range analysisRequests {

		analysisResults, err := as.analysisRepository.GetAnalysisResultsBySampleCodeAndAnalyteID(ctx, request.SampleCode, request.AnalyteID)
		if err != nil {
			log.Debug().Err(err).Str("requestID", request.ID.String()).Msg("Failed to load analysis results for the request")
			return err
		}

		processedAnalysisRequestIDs = append(processedAnalysisRequestIDs, request.ID)

		if len(analysisResults) < 1 {
			continue
		}

		completableRequestCount++

		for i := range analysisResults {
			analysisResults[i].AnalysisRequest = request
			as.manager.SendResultForProcessing(analysisResults[i])
		}
	}

	_ = as.analysisRepository.MarkAnalysisRequestsAsProcessed(ctx, processedAnalysisRequestIDs)

	log.Trace().Msgf("%d processed request(s) has results out from %d", completableRequestCount, len(analysisRequests))

	return nil
}

func (as *analysisService) RevokeAnalysisRequests(ctx context.Context, workItemIDs []uuid.UUID) error {
	log.Trace().Msgf("Revoking %d work-item(s) by IDs", len(workItemIDs))

	analysisRequests, err := as.analysisRepository.GetAnalysisRequestsByWorkItemIDs(ctx, workItemIDs)
	if err != nil {
		return ErrFailedToRevokeAnalysisRequests
	}

	as.manager.GetCallbackHandler().RevokeAnalysisRequests(analysisRequests)
	err = as.analysisRepository.DeleteAnalysisRequestExtraValues(ctx, workItemIDs)
	if err != nil {
		return ErrFailedToRevokeAnalysisRequests
	}
	return as.analysisRepository.RevokeAnalysisRequests(ctx, workItemIDs)
}

func (as *analysisService) ReexamineAnalysisRequestsBatch(ctx context.Context, workItemIDs []uuid.UUID) error {
	log.Trace().Msgf("Reexamine %d work-item(s) by IDs", len(workItemIDs))
	err := as.analysisRepository.IncreaseReexaminationRequestedCount(ctx, workItemIDs)
	if err != nil {
		return err
	}
	analysisRequests, err := as.analysisRepository.GetAnalysisRequestsByWorkItemIDs(ctx, workItemIDs)
	if err != nil {
		return err
	}
	as.manager.GetCallbackHandler().ReexamineAnalysisRequests(analysisRequests)

	return nil
}

func (as *analysisService) GetAnalysisRequestsInfo(ctx context.Context, instrumentID uuid.UUID, filter Filter) ([]AnalysisRequestInfo, int, error) {
	requestInfoList, totalCount, err := as.analysisRepository.GetAnalysisRequestsInfo(ctx, instrumentID, filter)
	if err != nil {
		return []AnalysisRequestInfo{}, 0, err
	}

	return requestInfoList, totalCount, nil
}

func (as *analysisService) GetAnalysisResultsInfo(ctx context.Context, instrumentID uuid.UUID, filter Filter) ([]AnalysisResultInfo, int, error) {
	resultInfoList, totalCount, err := as.analysisRepository.GetAnalysisResultsInfo(ctx, instrumentID, filter)
	if err != nil {
		return []AnalysisResultInfo{}, 0, err
	}

	return resultInfoList, totalCount, nil
}

func (as *analysisService) GetAnalysisBatches(ctx context.Context, instrumentID uuid.UUID, filter Filter) ([]AnalysisBatch, int, error) {
	analysisBatchList, totalCount, err := as.analysisRepository.GetAnalysisBatches(ctx, instrumentID, filter)
	if err != nil {
		return []AnalysisBatch{}, 0, err
	}

	return analysisBatchList, totalCount, nil
}

func (as *analysisService) QueueAnalysisResults(ctx context.Context, results []AnalysisResult) error {
	tx, err := as.analysisRepository.CreateTransaction()
	if err != nil {
		log.Error().Err(err).Msg("failed to create transaction")
		return err
	}

	_, err = as.analysisRepository.WithTransaction(tx).CreateAnalysisResultQueueItem(ctx, results)
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	analysisResultIDs := make([]uuid.UUID, 0)

	for _, result := range results {
		analysisResultIDs = append(analysisResultIDs, result.ID)
	}

	err = as.analysisRepository.WithTransaction(tx).MarkAnalysisResultsAsProcessed(ctx, analysisResultIDs)
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

func (as *analysisService) QueueControlResults(ctx context.Context, controlResults []MappedStandaloneControlResult) error {
	tx, err := as.analysisRepository.CreateTransaction()
	if err != nil {
		log.Error().Err(err).Msg("failed to create transaction")
		return err
	}

	controlResultToQueue := make([]StandaloneControlResult, 0)
	controlResultReagentRelationsToMarkAsProcessed := make(map[uuid.UUID][]uuid.UUID)
	controlResultAnalysisResultRelationsToMarkAsProcessed := make(map[uuid.UUID][]uuid.UUID)

	for _, controlResult := range controlResults {
		standaloneControlResult := StandaloneControlResult{
			ControlResult: controlResult.ControlResult,
			Reagents:      controlResult.Reagents,
		}

		for _, reagent := range controlResult.Reagents {
			controlResultReagentRelationsToMarkAsProcessed[controlResult.ID] = append(controlResultReagentRelationsToMarkAsProcessed[controlResult.ID], reagent.ID)
		}

		for analysisResultID, cerberusID := range controlResult.ResultIDs {
			standaloneControlResult.ResultIDs = append(standaloneControlResult.ResultIDs, cerberusID)
			controlResultAnalysisResultRelationsToMarkAsProcessed[controlResult.ID] = append(controlResultAnalysisResultRelationsToMarkAsProcessed[controlResult.ID], analysisResultID)
		}

		controlResultToQueue = append(controlResultToQueue, standaloneControlResult)
	}

	_, err = as.analysisRepository.WithTransaction(tx).CreateControlResultQueueItem(ctx, controlResultToQueue)
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	for controlResultID, reagentIDs := range controlResultReagentRelationsToMarkAsProcessed {
		err = as.analysisRepository.WithTransaction(tx).MarkReagentControlResultRelationsAsProcessed(ctx, controlResultID, reagentIDs)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
	}

	for controlResultID, analysisResultIDs := range controlResultAnalysisResultRelationsToMarkAsProcessed {
		err = as.analysisRepository.WithTransaction(tx).MarkAnalysisResultControlResultRelationsAsProcessed(ctx, controlResultID, analysisResultIDs)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
	}

	err = tx.Commit()
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	return nil
}

func (as *analysisService) RetransmitResult(ctx context.Context, resultID uuid.UUID) error {
	analysisResult, err := as.analysisRepository.GetAnalysisResultByID(ctx, resultID, true)
	if err != nil {
		return err
	}

	as.manager.SendResultForProcessing(analysisResult)

	return nil
}

func (as *analysisService) RetransmitResultBatches(ctx context.Context, batchIDs []uuid.UUID) error {
	log.Trace().Msgf("Trying to retransmit %d analysis results by CerberusID", len(batchIDs))

	analysisResults, err := as.analysisRepository.GetAnalysisResultsByBatchIDs(ctx, batchIDs)
	if err != nil {
		return err
	}

	log.Trace().Msgf("Retransmitting %d analysis results", len(analysisResults))

	for _, analysisResult := range analysisResults {
		as.manager.SendResultForProcessing(analysisResult)
	}

	return nil
}

const stuckImageBatchSize = 500

func (as *analysisService) ProcessStuckImagesToDEA(ctx context.Context) {
	stuckImageIDs, err := as.analysisRepository.GetStuckImageIDsForDEA(ctx)
	if err != nil {
		return
	}

	if len(stuckImageIDs) == 0 {
		return
	}

	_ = utils.Partition(len(stuckImageIDs), stuckImageBatchSize, func(low int, high int) error {
		images, err := as.analysisRepository.GetImagesForDEAUploadByIDs(ctx, stuckImageIDs[low:high])
		if err != nil {
			return err
		}

		for _, image := range images {
			deaImageID, err := as.deaClient.UploadImage(image.ImageBytes, image.Name)
			if err != nil {
				_ = as.analysisRepository.IncreaseImageUploadRetryCount(ctx, image.ID, err.Error())
				continue
			}

			_ = as.analysisRepository.SaveDEAImageID(ctx, image.ID, deaImageID)
		}

		return nil
	})
}

func (as *analysisService) ProcessStuckImagesToCerberus(ctx context.Context) {
	stuckImageIDs, err := as.analysisRepository.GetStuckImageIDsForCerberus(ctx)
	if err != nil {
		return
	}

	if len(stuckImageIDs) == 0 {
		return
	}

	_ = utils.Partition(len(stuckImageIDs), stuckImageBatchSize, func(low int, high int) error {
		imageDAOs, err := as.analysisRepository.GetImagesForCerberusSyncByIDs(ctx, stuckImageIDs[low:high])
		if err != nil {
			return err
		}

		imageTOs := make([]WorkItemResultImageTO, 0)

		for i := range imageDAOs {
			imageTO := WorkItemResultImageTO{
				WorkItemID: imageDAOs[i].WorkItemID,
				Image: ImageTO{
					ID:   imageDAOs[i].DeaImageID,
					Name: imageDAOs[i].Name,
				},
			}

			if imageDAOs[i].ChannelID.Valid {
				imageTO.ChannelID = &imageDAOs[i].ChannelID.UUID
			}

			if imageDAOs[i].YieldedAt.Valid {
				imageTO.ResultYieldDateTime = &imageDAOs[i].YieldedAt.Time
			}

			if imageDAOs[i].Description.Valid {
				imageTO.Image.Description = &imageDAOs[i].Description.String
			}

			imageTOs = append(imageTOs, imageTO)
		}

		err = as.cerberusClient.SendAnalysisResultImageBatch(imageTOs)
		if err != nil {
			return err
		}

		_ = as.analysisRepository.MarkImagesAsSyncedToCerberus(ctx, stuckImageIDs[low:high])

		return nil
	})
}

func (as *analysisService) SaveCerberusIDsForControlResultBatchItems(ctx context.Context, controlResults []ControlResultBatchItemInfo) {
	for _, controlResult := range controlResults {
		_ = as.analysisRepository.SaveCerberusIDForControlResult(ctx, controlResult.ControlResult.ID, controlResult.CerberusID)

		for i, reagent := range controlResult.ControlResult.Reagents {
			_ = as.analysisRepository.SaveCerberusIDForReagent(ctx, reagent.ID, controlResult.CerberusReagentIDs[i])
		}
	}
}

func (as *analysisService) SaveCerberusIDsForAnalysisResultBatchItems(ctx context.Context, analysisResults []AnalysisResultBatchItemInfo) {
	for _, analysisResult := range analysisResults {
		_ = as.analysisRepository.SaveCerberusIDForAnalysisResult(ctx, analysisResult.AnalysisResult.ID, *analysisResult.CerberusAnalysisResultID)

		for i, reagent := range analysisResult.AnalysisResult.Reagents {
			_ = as.analysisRepository.SaveCerberusIDForReagent(ctx, reagent.ID, analysisResult.CerberusReagentIDs[i].CerberusID)

			for j, controlResult := range reagent.ControlResults {
				_ = as.analysisRepository.SaveCerberusIDForControlResult(ctx, controlResult.ID, analysisResult.CerberusReagentIDs[i].CerberusControlResultsIDs[j])
			}
		}
	}
}

func (as *analysisService) GetUnprocessedMappedStandaloneControlResultsByIDs(ctx context.Context, controlResultIDs []uuid.UUID) ([]MappedStandaloneControlResult, error) {
	mappedControlResults := make([]MappedStandaloneControlResult, 0)

	unprocessedAnalysisResultIDsMappedByControlResultIDs, err := as.analysisRepository.GetUnprocessedAnalysisResultIDsByControlResultIDs(ctx, controlResultIDs)
	if err != nil {
		return mappedControlResults, err
	}

	unprocessedReagentIDsMappedByControlResultIDs, err := as.analysisRepository.GetUnprocessedReagentIDsByControlResultIDs(ctx, controlResultIDs)
	if err != nil {
		return mappedControlResults, err
	}

	reagentIDs := make([]uuid.UUID, 0)

	for _, rIDs := range unprocessedReagentIDsMappedByControlResultIDs {
		reagentIDs = append(reagentIDs, rIDs...)
	}

	reagentsMap, err := as.analysisRepository.GetReagentsByIDs(ctx, reagentIDs)
	if err != nil {
		return mappedControlResults, err
	}

	controlResults, err := as.analysisRepository.GetControlResultsByIDs(ctx, controlResultIDs)
	if err != nil {
		return mappedControlResults, err
	}

	for _, controlResult := range controlResults {
		mappedControlResult := MappedStandaloneControlResult{
			ControlResult: controlResult,
			ResultIDs:     unprocessedAnalysisResultIDsMappedByControlResultIDs[controlResult.ID],
		}

		for _, reagentID := range unprocessedReagentIDsMappedByControlResultIDs[controlResult.ID] {
			mappedControlResult.Reagents = append(mappedControlResult.Reagents, reagentsMap[reagentID])
		}

		if len(mappedControlResult.Reagents) > 0 || len(mappedControlResult.ResultIDs) > 0 {
			mappedControlResults = append(mappedControlResults, mappedControlResult)
		}
	}

	return mappedControlResults, nil
}
