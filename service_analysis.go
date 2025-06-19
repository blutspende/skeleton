package skeleton

import (
	"context"
	"errors"
	"fmt"
	"github.com/blutspende/bloodlab-common/utils"
	"github.com/blutspende/skeleton/db"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"time"
)

var (
	ErrAnalysisRequestWithMatchingWorkItemIdFound = errors.New("analysis request with matching workitem id found")
	ErrUnsupportedExpectedControlResultFound      = errors.New("unsupported expected control result operator found")
)

type AnalysisService interface {
	CreateAnalysisRequests(ctx context.Context, analysisRequests []AnalysisRequest) error
	ProcessAnalysisRequests(ctx context.Context, analysisRequests []AnalysisRequest) error
	RevokeAnalysisRequests(ctx context.Context, workItemIDs []uuid.UUID) error
	ReexamineAnalysisRequestsBatch(ctx context.Context, workItemIDs []uuid.UUID) error
	CreateAnalysisResultsBatch(ctx context.Context, analysisResults AnalysisResultSet) ([]AnalysisResult, error)
	CreateControlResultBatch(ctx context.Context, controlResults []StandaloneControlResult) ([]StandaloneControlResult, []uuid.UUID, error)
	GetAnalysisResultsByIDsWithRecalculatedStatus(ctx context.Context, analysisResultIDs []uuid.UUID, reValidateControlResult bool) ([]AnalysisResult, error)
	ValidateAndUpdatingExistingControlResults(ctx context.Context, analyteMappingIds []uuid.UUID) error
	AnalysisResultStatusRecalculationAndSendForProcessingIfFinal(ctx context.Context, controlResultIds []uuid.UUID) error
	QueueAnalysisResults(ctx context.Context, results []AnalysisResult) error
	RetransmitResult(ctx context.Context, resultID uuid.UUID) error
	RetransmitResultBatches(ctx context.Context, batchIDs []uuid.UUID) error
	ProcessStuckImagesToDEA(ctx context.Context)
	ProcessStuckImagesToCerberus(ctx context.Context)
	SaveCerberusIDsForAnalysisResultBatchItems(ctx context.Context, analysisResults []AnalysisResultBatchItemInfo)
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

func (as *analysisService) CreateAnalysisRequests(ctx context.Context, analysisRequests []AnalysisRequest) error {
	ts := time.Now().UTC()
	for i := range analysisRequests {
		if analysisRequests[i].ID == uuid.Nil {
			analysisRequests[i].ID = uuid.New()
		}
		analysisRequests[i].CreatedAt = ts
	}
	tx, err := as.analysisRepository.CreateTransaction()
	if err != nil {
		return err
	}
	savedIDs, err := as.analysisRepository.WithTransaction(tx).CreateAnalysisRequestsBatch(ctx, analysisRequests)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	savedIDsMap := make(map[uuid.UUID]any)
	for i := range savedIDs {
		savedIDsMap[savedIDs[i]] = nil
	}
	extraValuesMap := make(map[uuid.UUID][]ExtraValue)
	analysisRequestsToProcess := make([]AnalysisRequest, 0)
	for i := range analysisRequests {
		if _, ok := savedIDsMap[analysisRequests[i].ID]; !ok {
			continue
		}
		extraValuesMap[analysisRequests[i].ID] = analysisRequests[i].ExtraValues
		analysisRequestsToProcess = append(analysisRequestsToProcess, analysisRequests[i])
	}

	err = as.analysisRepository.WithTransaction(tx).CreateAnalysisRequestExtraValues(ctx, extraValuesMap)
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	err = tx.Commit()
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	as.manager.SendAnalysisRequestsForProcessing(analysisRequestsToProcess)

	return nil
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
		for i := range analysisResults {
			analysisResults[i], err = setAnalysisResultStatusBasedOnControlResults(analysisResults[i], nil, false)
			if err != nil {
				return err
			}
		}

		tx, err := as.analysisRepository.CreateTransaction()
		if err != nil {
			log.Error().Err(err).Msg("failed to create transaction")
			return err
		}

		err = as.analysisRepository.WithTransaction(tx).UpdateStatusAnalysisResultsBatch(ctx, analysisResults)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
		if err = tx.Commit(); err != nil {
			_ = tx.Rollback()
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
		return ErrRevokeAnalysisRequestsFailed
	}
	err = as.analysisRepository.DeleteAnalysisRequestExtraValues(ctx, workItemIDs)
	if err != nil {
		return ErrRevokeAnalysisRequestsFailed
	}
	err = as.analysisRepository.RevokeAnalysisRequests(ctx, workItemIDs)
	if err != nil {
		return ErrRevokeAnalysisRequestsFailed
	}
	as.manager.GetCallbackHandler().RevokeAnalysisRequests(analysisRequests)

	return nil
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
			savedAnalysisResults[i].Reagents[j].ControlResults = append(savedAnalysisResults[i].Reagents[j].ControlResults, savedAnalysisResults[i].ControlResults...)
		}
		savedAnalysisResults[i].ControlResults = nil
	}

	return savedAnalysisResults, nil
}

func (as *analysisService) createAnalysisResultsBatch(ctx context.Context, tx db.DbConnection, analysisResultSet AnalysisResultSet) (AnalysisResultSet, error) {
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
		analysisResultSet.Results[i], err = setAnalysisResultStatusBasedOnControlResults(analysisResultSet.Results[i], analysisResultSet.ControlResults, true)
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

	analysisResultSet.ControlResults, err = as.analysisRepository.WithTransaction(tx).CreateControlResultBatch(ctx, analysisResultSet.ControlResults)
	if err != nil {
		return analysisResultSet, err
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

	err = as.analysisRepository.WithTransaction(tx).CreateWarnings(ctx, warningsMap)
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

		analysisResultSet.Results[i].ControlResults, err = as.analysisRepository.WithTransaction(tx).CreateControlResultBatch(ctx, analysisResultSet.Results[i].ControlResults)
		if err != nil {
			return analysisResultSet, err
		}
	}

	controlResultsMap, reagentsMapWithIds, err := as.createReagentsByAnalysisResultID(ctx, tx, reagentsMap)
	if err != nil {
		return analysisResultSet, err
	}

	resultRelationsMap, err := as.analysisRepository.WithTransaction(tx).CreateControlResults(ctx, controlResultsMap)
	if err != nil {
		return analysisResultSet, err
	}
	for i, result := range analysisResultSet.Results {
		for j, reagent := range result.Reagents {
			analysisResultSet.Results[i].Reagents[j].ID = reagentsMapWithIds[result.ID][j].ID
			for k := range reagent.ControlResults {
				analysisResultSet.Results[i].Reagents[j].ControlResults[k].ID = resultRelationsMap[result.ID][reagent.ID][k]
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

	alreadyCreatedReagentControlRelationMap := make(map[uuid.UUID]map[uuid.UUID]bool)
	alreadyCreatedAnalysisResultControlRelationMap := make(map[uuid.UUID]map[uuid.UUID]bool)
	for _, analysisResult := range analysisResultSet.Results {
		reagentsToLink := make(map[uuid.UUID][]uuid.UUID)
		if _, ok := resultRelationsMap[analysisResult.ID]; ok {
			reagentsToLink = resultRelationsMap[analysisResult.ID]
		} else {
			continue
		}
		for i := range commonReagentIds {
			reagentsToLink[commonReagentIds[i]] = make([]uuid.UUID, 0)
		}
		for reagentID := range reagentsToLink {
			analysisResultReagentRelationDAOs = append(analysisResultReagentRelationDAOs, analysisResultReagentRelationDAO{
				AnalysisResultID: analysisResult.ID,
				ReagentID:        reagentID,
			})
			controlResultsToLink := resultRelationsMap[analysisResult.ID][reagentID]
			controlResultsToLink = append(controlResultsToLink, commonControlResultIds...)
			for _, controlResult := range analysisResult.ControlResults {
				controlResultsToLink = append(controlResultsToLink, controlResult.ID)
			}

			for _, controlResultID := range controlResultsToLink {
				if !alreadyCreatedReagentControlRelationMap[reagentID][controlResultID] {
					if _, ok := alreadyCreatedReagentControlRelationMap[reagentID]; !ok {
						alreadyCreatedReagentControlRelationMap[reagentID] = make(map[uuid.UUID]bool)
					}
					reagentControlResultRelationDAOs = append(reagentControlResultRelationDAOs, reagentControlResultRelationDAO{
						ReagentID:       reagentID,
						ControlResultID: controlResultID,
						IsProcessed:     true,
					})
					alreadyCreatedReagentControlRelationMap[reagentID][controlResultID] = true
				}

				if !alreadyCreatedAnalysisResultControlRelationMap[analysisResult.ID][controlResultID] {
					if _, ok := alreadyCreatedAnalysisResultControlRelationMap[analysisResult.ID]; !ok {
						alreadyCreatedAnalysisResultControlRelationMap[analysisResult.ID] = make(map[uuid.UUID]bool)
					}
					analysisResultControlResultRelationDAOs = append(analysisResultControlResultRelationDAOs, analysisResultControlResultRelationDAO{
						AnalysisResultID: analysisResult.ID,
						ControlResultID:  controlResultID,
						IsProcessed:      true,
					})
					alreadyCreatedAnalysisResultControlRelationMap[analysisResult.ID][controlResultID] = true
				}
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

func setAnalysisResultStatusBasedOnControlResults(analysisResult AnalysisResult, commonControlResults []ControlResult, reValidateControlResult bool) (AnalysisResult, error) {
	analysisResult.Status = Preliminary
	if analysisResult.AnalyteMapping.ControlResultRequired {
		controlSampleCodeMap := make(map[string]bool)
		expectedControlSampleCodeMap := make(map[string]bool)
		for j := range analysisResult.AnalyteMapping.ExpectedControlResults {
			expectedControlSampleCodeMap[analysisResult.AnalyteMapping.ExpectedControlResults[j].SampleCode] = false
		}

		for j := range analysisResult.Reagents {
			for k := range analysisResult.Reagents[j].ControlResults {
				var controlResult ControlResult
				if reValidateControlResult {
					var err error
					controlResult, err = setControlResultIsValidAndExpectedControlResultId(analysisResult.Reagents[j].ControlResults[k])
					if err != nil {
						return analysisResult, err
					}
					analysisResult.Reagents[j].ControlResults[k] = controlResult
				} else {
					controlResult = analysisResult.Reagents[j].ControlResults[k]
				}

				if _, ok := controlSampleCodeMap[controlResult.SampleCode]; !ok {
					controlSampleCodeMap[controlResult.SampleCode] = false
				}
			}
		}

		for j := range analysisResult.ControlResults {
			var controlResult ControlResult
			if reValidateControlResult {
				var err error
				controlResult, err = setControlResultIsValidAndExpectedControlResultId(analysisResult.ControlResults[j])
				if err != nil {
					return analysisResult, err
				}
				analysisResult.ControlResults[j] = controlResult
			} else {
				controlResult = analysisResult.ControlResults[j]
			}

			if _, ok := controlSampleCodeMap[controlResult.SampleCode]; !ok {
				controlSampleCodeMap[controlResult.SampleCode] = false
			}
		}

		for j := range commonControlResults {
			if _, ok := controlSampleCodeMap[commonControlResults[j].SampleCode]; !ok {
				controlSampleCodeMap[commonControlResults[j].SampleCode] = false
			}
		}

		var allExpectedControlHasControlResults = true

		if len(expectedControlSampleCodeMap) == 0 || len(expectedControlSampleCodeMap) != len(controlSampleCodeMap) {
			return analysisResult, nil
		}
		for controlSampleCode := range controlSampleCodeMap {
			for expectedControlSampleCode := range expectedControlSampleCodeMap {
				if controlSampleCode == expectedControlSampleCode {
					controlSampleCodeMap[controlSampleCode] = true
					expectedControlSampleCodeMap[expectedControlSampleCode] = true
					break
				}
			}
			if !controlSampleCodeMap[controlSampleCode] {
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
	log.Error().Err(ErrUnsupportedExpectedControlResultFound)
	return false, ErrUnsupportedExpectedControlResultFound
}

func (as *analysisService) createReagentsByAnalysisResultID(ctx context.Context, tx db.DbConnection, reagentsByAnalysisResultID map[uuid.UUID][]Reagent) (map[uuid.UUID]map[uuid.UUID][]ControlResult, map[uuid.UUID][]Reagent, error) {
	reagentList := make([]Reagent, 0)
	analysisResultIDsProcessingOrder := make([]uuid.UUID, 0)
	controlResultsMap := make(map[uuid.UUID]map[uuid.UUID][]ControlResult)
	for analysisResultID, reagents := range reagentsByAnalysisResultID {
		for i := range reagents {
			reagentList = append(reagentList, reagents[i])
		}

		analysisResultIDsProcessingOrder = append(analysisResultIDsProcessingOrder, analysisResultID)
	}

	reagentIDs, err := as.analysisRepository.WithTransaction(tx).CreateReagents(ctx, reagentList)
	if err != nil {
		return nil, nil, err
	}

	var index = 0
	for _, analysisResultID := range analysisResultIDsProcessingOrder {
		reagents := reagentsByAnalysisResultID[analysisResultID]
		for j, reagent := range reagents {
			if _, ok := controlResultsMap[analysisResultID]; !ok {
				controlResultsMap[analysisResultID] = make(map[uuid.UUID][]ControlResult)
			}

			controlResultsMap[analysisResultID][reagentIDs[index]] = reagent.ControlResults
			reagentsByAnalysisResultID[analysisResultID][j].ID = reagentIDs[index]

			index++
		}
	}

	return controlResultsMap, reagentsByAnalysisResultID, nil
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

func (as *analysisService) createControlResultBatch(ctx context.Context, tx db.DbConnection, standaloneControlResults []StandaloneControlResult) ([]StandaloneControlResult, []uuid.UUID, error) {
	crs := make([]ControlResult, len(standaloneControlResults))
	analysisResultIDs := make([]uuid.UUID, 0)
	reagents := make([]Reagent, 0)

	var err error
	for i, controlResult := range standaloneControlResults {
		crs[i], err = setControlResultIsValidAndExpectedControlResultId(standaloneControlResults[i].ControlResult)
		if err != nil {
			_ = tx.Rollback()
			return standaloneControlResults, analysisResultIDs, err
		}
		standaloneControlResults[i].ControlResult = crs[i]
		analysisResultIDs = append(analysisResultIDs, controlResult.ResultIDs...)
		reagents = append(reagents, standaloneControlResults[i].Reagents...)
	}

	controlResults, err := as.analysisRepository.WithTransaction(tx).CreateControlResultBatch(ctx, crs)
	if err != nil {
		return standaloneControlResults, analysisResultIDs, err
	}

	reagentIDs, err := as.analysisRepository.WithTransaction(tx).CreateReagents(ctx, reagents)
	if err != nil {
		return standaloneControlResults, analysisResultIDs, err
	}

	for i := range controlResults {
		standaloneControlResults[i].ID = controlResults[i].ID
	}

	reagentControlResultRelationDAOs := make([]reagentControlResultRelationDAO, 0)
	analysisResultControlResultRelationDAOs := make([]analysisResultControlResultRelationDAO, 0)

	var reagentIndex = 0

	for i := range controlResults {
		standaloneControlResults[i].ID = controlResults[i].ID

		for j := range standaloneControlResults[i].Reagents {
			standaloneControlResults[i].Reagents[j].ID = reagentIDs[reagentIndex]

			reagentControlResultRelationDAOs = append(reagentControlResultRelationDAOs, reagentControlResultRelationDAO{ReagentID: reagentIDs[reagentIndex], ControlResultID: controlResults[i].ID, IsProcessed: false})

			reagentIndex++
		}

		for _, resultID := range standaloneControlResults[i].ResultIDs {
			analysisResultControlResultRelationDAOs = append(analysisResultControlResultRelationDAOs, analysisResultControlResultRelationDAO{AnalysisResultID: resultID, ControlResultID: controlResults[i].ID, IsProcessed: false})
		}
	}

	err = as.analysisRepository.WithTransaction(tx).CreateReagentControlResultRelations(ctx, reagentControlResultRelationDAOs)
	if err != nil {
		return standaloneControlResults, analysisResultIDs, err
	}

	err = as.analysisRepository.WithTransaction(tx).CreateAnalysisResultControlResultRelations(ctx, analysisResultControlResultRelationDAOs)
	if err != nil {
		return standaloneControlResults, analysisResultIDs, err
	}

	return standaloneControlResults, analysisResultIDs, nil
}

func (as *analysisService) GetAnalysisResultsByIDsWithRecalculatedStatus(ctx context.Context, analysisResultIDs []uuid.UUID, reValidateControlResult bool) ([]AnalysisResult, error) {
	if len(analysisResultIDs) == 0 {
		return []AnalysisResult{}, nil
	}
	analysisResults, err := as.analysisRepository.GetAnalysisResultsByIDs(ctx, analysisResultIDs)
	if err != nil {
		log.Error().Err(err).Msg("get analysis results by ids failed after saving results")
		return analysisResults, err
	}

	for i := range analysisResults {
		analysisResults[i], err = setAnalysisResultStatusBasedOnControlResults(analysisResults[i], nil, reValidateControlResult)
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

func (as *analysisService) ValidateAndUpdatingExistingControlResults(ctx context.Context, analyteMappingIds []uuid.UUID) error {
	controlResults, err := as.analysisRepository.GetControlResultsToValidate(ctx, analyteMappingIds)
	if err != nil {
		return err
	}

	if len(controlResults) == 0 {
		return nil
	}

	updatedControlResultIds := make([]uuid.UUID, 0)
	for i := range controlResults {
		cr, err := setControlResultIsValidAndExpectedControlResultId(controlResults[i])
		if err == nil {
			controlResults[i] = cr
			updatedControlResultIds = append(updatedControlResultIds, controlResults[i].ID)
		}
	}

	tx, err := as.analysisRepository.CreateTransaction()
	if err != nil {
		log.Error().Err(err).Msg("failed to create transaction")
		return err
	}

	err = as.analysisRepository.WithTransaction(tx).UpdateControlResultBatch(ctx, controlResults)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	if err = tx.Commit(); err != nil {
		_ = tx.Rollback()
		return err
	}

	as.manager.SendControlResultIdsToAnalysisResultStatusRecalculation(updatedControlResultIds)

	return nil
}

func (as *analysisService) AnalysisResultStatusRecalculationAndSendForProcessingIfFinal(ctx context.Context, controlResultIds []uuid.UUID) error {
	analysisResultIds, err := as.analysisRepository.GetAnalysisResultIdsForStatusRecalculationByControlIds(ctx, controlResultIds)
	if err != nil {
		return err
	}

	updatedAnalysisResults, err := as.GetAnalysisResultsByIDsWithRecalculatedStatus(ctx, analysisResultIds, false)
	if err != nil {
		return err
	}

	go func(analysisResults []AnalysisResult) {
		for i := range analysisResults {
			analyteRequests, err := as.analysisRepository.GetAnalysisRequestsBySampleCodeAndAnalyteID(ctx, analysisResults[i].SampleCode, analysisResults[i].AnalyteMapping.AnalyteID)
			if err != nil {
				log.Error().Err(err).Msg("get analysis requests by sample code and analyteID failed after saving results for resubmitting analysis results after recalculated status")
				return
			}

			for j := range analyteRequests {
				analysisResults[i].AnalysisRequest = analyteRequests[j]
				as.manager.SendResultForProcessing(analysisResults[i])
			}
		}
	}(updatedAnalysisResults)

	return nil
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

	analysisResultIDsToMarkAsProcessed := make([]uuid.UUID, 0)
	controlResultReagentRelationsToMarkAsProcessed := make(map[uuid.UUID][]uuid.UUID)
	controlResultAnalysisResultRelationsToMarkAsProcessed := make(map[uuid.UUID][]uuid.UUID)

	for _, result := range results {
		analysisResultIDsToMarkAsProcessed = append(analysisResultIDsToMarkAsProcessed, result.ID)

		for _, reagent := range result.Reagents {

			for _, controlResult := range reagent.ControlResults {
				controlResultReagentRelationsToMarkAsProcessed[controlResult.ID] = append(controlResultReagentRelationsToMarkAsProcessed[controlResult.ID], reagent.ID)
				controlResultAnalysisResultRelationsToMarkAsProcessed[controlResult.ID] = append(controlResultAnalysisResultRelationsToMarkAsProcessed[controlResult.ID], result.ID)
			}
		}
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

	err = as.analysisRepository.WithTransaction(tx).MarkAnalysisResultsAsProcessed(ctx, analysisResultIDsToMarkAsProcessed)
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

func (as *analysisService) RetransmitResult(ctx context.Context, resultID uuid.UUID) error {
	analysisResult, err := as.analysisRepository.GetAnalysisResultByCerberusID(ctx, resultID, true)
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

func (as *analysisService) SaveCerberusIDsForAnalysisResultBatchItems(ctx context.Context, analysisResults []AnalysisResultBatchItemInfo) {
	for _, analysisResult := range analysisResults {
		if len(analysisResult.ErrorMessage) != 0 {
			log.Warn().Msgf("Possible error happened in Cerberus at saving AnalysisResult with ID: %s Error: %s", analysisResult.AnalysisResult.ID.String(), analysisResult.ErrorMessage)
		}
		if analysisResult.CerberusAnalysisResultID != nil {
			_ = as.analysisRepository.SaveCerberusIDForAnalysisResult(ctx, analysisResult.AnalysisResult.ID, *analysisResult.CerberusAnalysisResultID)
		}
	}
}
