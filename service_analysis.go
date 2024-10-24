package skeleton

import (
	"context"
	"errors"
	"fmt"
	"github.com/blutspende/skeleton/db"
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
	CreateAnalysisResultsBatch(ctx context.Context, analysisResults AnalysisResultSet) ([]AnalysisResult, error)
	CreateControlResultBatch(ctx context.Context, controlResults []StandaloneControlResult) ([]StandaloneControlResult, []uuid.UUID, error)
	GetAnalysisResultsByIDsWithRecalculatedStatus(ctx context.Context, analysisResultIDs []uuid.UUID) ([]AnalysisResult, error)
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

func (as *analysisService) CreateAnalysisResultsBatch(ctx context.Context, analysisResults AnalysisResultSet) ([]AnalysisResult, error) {
	tx, err := as.analysisRepository.CreateTransaction()
	if err != nil {
		log.Error().Err(err).Msg("failed to create transaction")
		return nil, err
	}
	savedResultDataList, err := as.createAnalysisResultsBatch(ctx, tx, analysisResults)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		_ = tx.Rollback()
		return nil, err
	}

	savedAnalysisResults := savedResultDataList.Results
	for i := range savedAnalysisResults {
		savedAnalysisResults[i].Reagents = append(savedAnalysisResults[i].Reagents, savedResultDataList.Reagents...)
		for j := range savedAnalysisResults[i].Reagents {
			if savedAnalysisResults[i].Reagents[j].ControlResults == nil {
				savedAnalysisResults[i].Reagents[j].ControlResults = make([]ControlResult, 0)
			}
			savedAnalysisResults[i].Reagents[j].ControlResults = append(savedAnalysisResults[i].Reagents[j].ControlResults, savedResultDataList.ControlResults...)
		}
	}

	return savedAnalysisResults, nil
}

func (as *analysisService) createAnalysisResultsBatch(ctx context.Context, tx db.DbConnector, analysisResultSet AnalysisResultSet) (AnalysisResultSet, error) {
	var err error
	for i := range analysisResultSet.Results {
		if analysisResultSet.Results[i].AnalyteMapping.ID == uuid.Nil {
			return analysisResultSet, errors.New(fmt.Sprintf("analyte mapping CerberusID is missing at index: %d", i))
		}
		if analysisResultSet.Results[i].Instrument.ID == uuid.Nil {
			return analysisResultSet, errors.New(fmt.Sprintf("instrument CerberusID is missing at index: %d", i))
		}
		if analysisResultSet.Results[i].ResultMode == "" {
			analysisResultSet.Results[i].ResultMode = analysisResultSet.Results[i].Instrument.ResultMode
		}
		analysisResultSet.Results[i], err = setAnalysisResultStatusBasedOnControlResults(analysisResultSet.Results[i])
		if err != nil {
			return analysisResultSet, err
		}
	}

	analysisResultSet.Results, err = as.analysisRepository.WithTransaction(tx).CreateAnalysisResultsBatch(ctx, analysisResultSet.Results)
	if err != nil {
		return analysisResultSet, err
	}
	analysisResultSet.Reagents, err = as.analysisRepository.WithTransaction(tx).CreateReagentBatch(ctx, analysisResultSet.Reagents)
	if err != nil {
		return analysisResultSet, err
	}

	for i := range analysisResultSet.ControlResults {
		analysisResultSet.ControlResults[i], err = setControlResultIsValidAndExpectedControlResultId(analysisResultSet.ControlResults[i])
		if err != nil {
			return analysisResultSet, err
		}
	}

	controlIds, err := as.analysisRepository.WithTransaction(tx).CreateControlResultBatch(ctx, analysisResultSet.ControlResults)
	if err != nil {
		return analysisResultSet, err
	}
	for i := range analysisResultSet.ControlResults {
		analysisResultSet.ControlResults[i].ID = controlIds[i]
	}

	extraValuesMap := make(map[uuid.UUID][]ExtraValue)
	warningsMap := make(map[uuid.UUID][]string)
	reagentsMap := make(map[uuid.UUID][]Reagent)

	for i := range analysisResultSet.Results {
		extraValuesMap[analysisResultSet.Results[i].ID] = analysisResultSet.Results[i].ExtraValues
		warningsMap[analysisResultSet.Results[i].ID] = analysisResultSet.Results[i].Warnings
		reagentsMap[analysisResultSet.Results[i].ID] = analysisResultSet.Results[i].Reagents
	}

	err = as.analysisRepository.WithTransaction(tx).CreateAnalysisResultExtraValues(ctx, extraValuesMap)
	if err != nil {
		return analysisResultSet, err
	}

	for i := range analysisResultSet.Results {
		quantitativeChannelResultsMap := make(map[uuid.UUID]map[string]string)
		channelImagesMap := make(map[uuid.NullUUID][]Image)
		channelResultIDs, err := as.analysisRepository.WithTransaction(tx).CreateChannelResults(ctx, analysisResultSet.Results[i].ChannelResults, analysisResultSet.Results[i].ID)
		if err != nil {
			return analysisResultSet, err
		}
		for j := range analysisResultSet.Results[i].ChannelResults {
			if len(channelResultIDs) > j {
				analysisResultSet.Results[i].ChannelResults[j].ID = channelResultIDs[j]
			}
			quantitativeChannelResultsMap[channelResultIDs[j]] = analysisResultSet.Results[i].ChannelResults[j].QuantitativeResults
			channelImagesMap[uuid.NullUUID{UUID: channelResultIDs[j], Valid: true}] = analysisResultSet.Results[i].ChannelResults[j].Images
		}
		err = as.analysisRepository.WithTransaction(tx).CreateChannelResultQuantitativeValues(ctx, quantitativeChannelResultsMap)
		if err != nil {
			return analysisResultSet, err
		}
	}

	err = as.analysisRepository.WithTransaction(tx).CreateWarnings(ctx, warningsMap)
	if err != nil {
		return analysisResultSet, err
	}

	controlResultsMap, reagentsMapWithIds, err := as.analysisRepository.WithTransaction(tx).CreateReagentsByAnalysisResultID(ctx, reagentsMap)
	if err != nil {
		return analysisResultSet, err
	}

	relationsMap, err := as.analysisRepository.WithTransaction(tx).CreateControlResults(ctx, controlResultsMap)
	if err != nil {
		return analysisResultSet, err
	}
	for i, result := range analysisResultSet.Results {
		for j, reagent := range result.Reagents {
			analysisResultSet.Results[i].Reagents[j].ID = reagentsMapWithIds[result.ID][j].ID
			for k := range reagent.ControlResults {
				analysisResultSet.Results[i].Reagents[j].ControlResults[k].ID = relationsMap[result.ID][reagent.ID][k]
			}
		}
	}

	analysisResultReagentRelationDAOs := make([]analysisResultReagentRelationDAO, 0)
	reagentControlResultRelationDAOs := make([]reagentControlResultRelationDAO, 0)
	analysisResultControlResultRelationDAOs := make([]analysisResultControlResultRelationDAO, 0)

	commonReagentIds := make([]uuid.UUID, 0)
	commonControlResultIds := make([]uuid.UUID, 0)
	for i := range analysisResultSet.Reagents {
		commonReagentIds = append(commonReagentIds, analysisResultSet.Reagents[i].ID)
	}
	for i := range analysisResultSet.ControlResults {
		commonControlResultIds = append(commonControlResultIds, analysisResultSet.ControlResults[i].ID)
	}

	for analysisResultID := range relationsMap {

		reagentsToLink := relationsMap[analysisResultID]
		for i := range commonReagentIds {
			reagentsToLink[commonReagentIds[i]] = make([]uuid.UUID, 0)
		}
		for reagentID := range reagentsToLink {
			analysisResultReagentRelationDAOs = append(analysisResultReagentRelationDAOs, analysisResultReagentRelationDAO{
				AnalysisResultID: analysisResultID,
				ReagentID:        reagentID,
			})
			controlResultsToLink := relationsMap[analysisResultID][reagentID]
			controlResultsToLink = append(controlResultsToLink, commonControlResultIds...)

			for _, controlResultID := range controlResultsToLink {
				reagentControlResultRelationDAOs = append(reagentControlResultRelationDAOs, reagentControlResultRelationDAO{
					ReagentID:       reagentID,
					ControlResultID: controlResultID,
					IsProcessed:     true,
				})

				analysisResultControlResultRelationDAOs = append(analysisResultControlResultRelationDAOs, analysisResultControlResultRelationDAO{
					AnalysisResultID: analysisResultID,
					ControlResultID:  controlResultID,
					IsProcessed:      true,
				})

			}
		}
	}

	err = as.analysisRepository.WithTransaction(tx).CreateAnalysisResultReagentRelations(ctx, analysisResultReagentRelationDAOs)
	if err != nil {
		return analysisResultSet, err
	}

	err = as.analysisRepository.WithTransaction(tx).CreateReagentControlResultRelations(ctx, reagentControlResultRelationDAOs)
	if err != nil {
		return analysisResultSet, err
	}

	err = as.analysisRepository.WithTransaction(tx).CreateAnalysisResultControlResultRelations(ctx, analysisResultControlResultRelationDAOs)
	if err != nil {
		return analysisResultSet, err
	}

	return analysisResultSet, nil
}

func setAnalysisResultStatusBasedOnControlResults(analysisResult AnalysisResult) (AnalysisResult, error) {
	analysisResult.Status = Preliminary
	if analysisResult.AnalyteMapping.ControlResultRequired {
		controlSampleCodeMap := make(map[string]bool)
		for j := range analysisResult.AnalyteMapping.ExpectedControlResults {
			controlSampleCodeMap[analysisResult.AnalyteMapping.ExpectedControlResults[j].SampleCode] = false
		}

		for j := range analysisResult.Reagents {
			for k := range analysisResult.Reagents[j].ControlResults {
				controlResult, err := setControlResultIsValidAndExpectedControlResultId(analysisResult.Reagents[j].ControlResults[k])
				if err != nil {
					return analysisResult, err
				}
				analysisResult.Reagents[j].ControlResults[k] = controlResult
				if _, ok := controlSampleCodeMap[controlResult.SampleCode]; ok {
					controlSampleCodeMap[controlResult.SampleCode] = true
				}
			}
		}

		for j := range analysisResult.ControlResults {
			controlResult, err := setControlResultIsValidAndExpectedControlResultId(analysisResult.ControlResults[j])
			if err != nil {
				return analysisResult, err
			}
			analysisResult.ControlResults[j] = controlResult
			if _, ok := controlSampleCodeMap[controlResult.SampleCode]; ok {
				controlSampleCodeMap[controlResult.SampleCode] = true
			}
		}

		var allExpectedControlHasControlResults = true
		for _, boolValue := range controlSampleCodeMap {
			if !boolValue {
				allExpectedControlHasControlResults = false
				break
			}
		}
		if allExpectedControlHasControlResults {
			analysisResult.Status = Final
		}
	} else {
		analysisResult.Status = Final
	}
	return analysisResult, nil
}

func setControlResultIsValidAndExpectedControlResultId(controlResult ControlResult) (ControlResult, error) {
	controlResult.IsValid = false
	controlResult.IsComparedToExpectedResult = false
	for i, expectedControlResult := range controlResult.AnalyteMapping.ExpectedControlResults {
		if expectedControlResult.SampleCode == controlResult.SampleCode {
			isValid, err := calculateControlResultIsValid(controlResult.Result, controlResult.AnalyteMapping.ExpectedControlResults[i])
			if err != nil {
				return controlResult, err
			}
			controlResult.IsValid = isValid
			controlResult.ExpectedControlResultId = uuid.NullUUID{
				UUID:  controlResult.AnalyteMapping.ExpectedControlResults[i].ID,
				Valid: true,
			}
			controlResult.IsComparedToExpectedResult = true
			break
		}
	}

	return controlResult, nil
}

func calculateControlResultIsValid(controlResult string, expectedControlResult ExpectedControlResult) (bool, error) {
	switch expectedControlResult.Operator {
	case Equals:
		return controlResult == expectedControlResult.ExpectedValue, nil
	case NotEquals:
		return controlResult != expectedControlResult.ExpectedValue, nil
	case Greater:
		return controlResult > expectedControlResult.ExpectedValue, nil
	case GreaterOrEqual:
		return controlResult >= expectedControlResult.ExpectedValue, nil
	case Less:
		return controlResult < expectedControlResult.ExpectedValue, nil
	case LessOrEqual:
		return controlResult <= expectedControlResult.ExpectedValue, nil
	case InOpenInterval:
		return controlResult >= expectedControlResult.ExpectedValue && controlResult <= *expectedControlResult.ExpectedValue2, nil
	case InClosedInterval:
		return controlResult > expectedControlResult.ExpectedValue && controlResult < *expectedControlResult.ExpectedValue2, nil
	}
	log.Error().Msg("unsupported expected control result operator found")
	return false, errors.New("unsupported expected control result operator found")
}

func (as *analysisService) CreateControlResultBatch(ctx context.Context, controlResults []StandaloneControlResult) ([]StandaloneControlResult, []uuid.UUID, error) {
	tx, err := as.analysisRepository.CreateTransaction()
	var analysisResultIds []uuid.UUID
	if err != nil {
		return controlResults, analysisResultIds, err
	}

	controlResults, analysisResultIds, err = as.createControlResultBatch(ctx, tx, controlResults)
	if err != nil {
		_ = tx.Rollback()
		return controlResults, analysisResultIds, err
	}

	if err = tx.Commit(); err != nil {
		_ = tx.Rollback()
		return controlResults, analysisResultIds, err
	}
	return controlResults, analysisResultIds, nil
}

func (as *analysisService) createControlResultBatch(ctx context.Context, tx db.DbConnector, controlResults []StandaloneControlResult) ([]StandaloneControlResult, []uuid.UUID, error) {
	crs := make([]ControlResult, len(controlResults))
	analysisResultIDs := make([]uuid.UUID, 0)
	reagents := make([]Reagent, 0)

	var err error
	for i, controlResult := range controlResults {
		crs[i], err = setControlResultIsValidAndExpectedControlResultId(controlResults[i].ControlResult)
		if err != nil {
			_ = tx.Rollback()
			return controlResults, analysisResultIDs, err
		}
		controlResults[i].ControlResult = crs[i]
		analysisResultIDs = append(analysisResultIDs, controlResult.ResultIDs...)
		reagents = append(reagents, controlResults[i].Reagents...)
	}

	crIDs, err := as.analysisRepository.WithTransaction(tx).CreateControlResultBatch(ctx, crs)
	if err != nil {
		return controlResults, analysisResultIDs, err
	}

	reagentIDs, err := as.analysisRepository.WithTransaction(tx).CreateReagents(ctx, reagents)
	if err != nil {
		return controlResults, analysisResultIDs, err
	}

	for i := range crIDs {
		controlResults[i].ID = crIDs[i]
	}

	reagentControlResultRelationDAOs := make([]reagentControlResultRelationDAO, 0)
	analysisResultControlResultRelationDAOs := make([]analysisResultControlResultRelationDAO, 0)

	var reagentIndex = 0

	for i := range crIDs {
		controlResults[i].ID = crIDs[i]

		for j := range controlResults[i].Reagents {
			controlResults[i].Reagents[j].ID = reagentIDs[reagentIndex]

			reagentControlResultRelationDAOs = append(reagentControlResultRelationDAOs, reagentControlResultRelationDAO{ReagentID: reagentIDs[reagentIndex], ControlResultID: crIDs[i], IsProcessed: false})

			reagentIndex++
		}

		for _, resultID := range controlResults[i].ResultIDs {
			analysisResultControlResultRelationDAOs = append(analysisResultControlResultRelationDAOs, analysisResultControlResultRelationDAO{AnalysisResultID: resultID, ControlResultID: crIDs[i], IsProcessed: false})
		}
	}

	err = as.analysisRepository.WithTransaction(tx).CreateReagentControlResultRelations(ctx, reagentControlResultRelationDAOs)
	if err != nil {
		return controlResults, analysisResultIDs, err
	}

	err = as.analysisRepository.WithTransaction(tx).CreateAnalysisResultControlResultRelations(ctx, analysisResultControlResultRelationDAOs)
	if err != nil {
		return controlResults, analysisResultIDs, err
	}

	return controlResults, analysisResultIDs, nil
}

func (as *analysisService) GetAnalysisResultsByIDsWithRecalculatedStatus(ctx context.Context, analysisResultIDs []uuid.UUID) ([]AnalysisResult, error) {
	analysisResults, err := as.analysisRepository.GetAnalysisResultsByIDs(ctx, analysisResultIDs)
	if err != nil {
		log.Error().Err(err).Msg("get analysis results by ids failed after saving results")
		return analysisResults, err
	}

	for i := range analysisResults {
		analysisResults[i], err = setAnalysisResultStatusBasedOnControlResults(analysisResults[i])
		if err != nil {
			return analysisResults, err
		}
	}

	tx, err := as.analysisRepository.CreateTransaction()
	if err != nil {
		log.Error().Err(err).Msg("failed to create transaction")
		return make([]AnalysisResult, 0), err
	}

	err = as.analysisRepository.WithTransaction(tx).UpdateStatusAnalysisResultsBatch(ctx, analysisResults)
	if err != nil {
		_ = tx.Rollback()
		return analysisResults, err
	}
	if err = tx.Commit(); err != nil {
		_ = tx.Rollback()
		return analysisResults, err
	}
	return analysisResults, nil
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
