package skeleton

import (
	"context"
	"github.com/DRK-Blutspende-BaWueHe/skeleton/utils"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

type AnalysisService interface {
	CreateAnalysisRequests(ctx context.Context, analysisRequests []AnalysisRequest) ([]AnalysisRequestStatus, error)
	ProcessAnalysisRequests(ctx context.Context, analysisRequests []AnalysisRequest) error
	RevokeAnalysisRequests(ctx context.Context, workItemIDs []uuid.UUID) error
	ReexamineAnalysisRequestsBatch(ctx context.Context, workItemIDs []uuid.UUID) error
	GetAnalysisRequestsInfo(ctx context.Context, instrumentID uuid.UUID, filter Filter) ([]AnalysisRequestInfo, int, error)
	GetAnalysisResultsInfo(ctx context.Context, instrumentID uuid.UUID, filter Filter) ([]AnalysisResultInfo, int, error)
	GetAnalysisBatches(ctx context.Context, instrumentID uuid.UUID, filter Filter) ([]AnalysisBatch, int, error)
	RetransmitResult(ctx context.Context, resultID uuid.UUID) error
	RetransmitResultBatches(ctx context.Context, batchIDs []uuid.UUID) error
	ProcessStuckImagesToDEA(ctx context.Context)
	ProcessStuckImagesToCerberus(ctx context.Context)
}

type analysisService struct {
	analysisRepository AnalysisRepository
	deaClient          DeaClientV1
	cerberusClient     Cerberus
	manager            Manager
}

func NewAnalysisService(analysisRepository AnalysisRepository, deaClient DeaClientV1, cerberusClient Cerberus, manager Manager) AnalysisService {
	return &analysisService{
		analysisRepository: analysisRepository,
		deaClient:          deaClient,
		cerberusClient:     cerberusClient,
		manager:            manager,
	}
}

func (as *analysisService) CreateAnalysisRequests(ctx context.Context, analysisRequests []AnalysisRequest) ([]AnalysisRequestStatus, error) {
	_, savedAnalysisRequestWorkItemIDs, err := as.analysisRepository.CreateAnalysisRequestsBatch(ctx, analysisRequests)
	if err != nil {
		return nil, err
	}

	as.manager.SendAnalysisRequestsForProcessing(analysisRequests)

	analysisRequestStatuses := make([]AnalysisRequestStatus, len(savedAnalysisRequestWorkItemIDs))
	for i := range savedAnalysisRequestWorkItemIDs {
		analysisRequestStatuses[i] = AnalysisRequestStatus{
			WorkItemID: savedAnalysisRequestWorkItemIDs[i],
			Error:      nil,
		}
	}

	return analysisRequestStatuses, nil
}

func (as *analysisService) ProcessAnalysisRequests(ctx context.Context, analysisRequests []AnalysisRequest) error {
	completableRequestCount := 0
	for _, request := range analysisRequests {
		analysisResults, err := as.analysisRepository.GetAnalysisResultsBySampleCodeAndAnalyteID(ctx, request.SampleCode, request.AnalyteID)
		if err != nil {
			log.Debug().Err(err).Str("requestID", request.ID.String()).Msg("Failed to load analysis results for the request")
			return err
		}

		if len(analysisResults) < 1 {
			continue
		}

		completableRequestCount++

		for i := range analysisResults {
			analysisResults[i].AnalysisRequest = request
			as.manager.SendResultForProcessing(analysisResults[i])
		}
	}

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

	return as.analysisRepository.RevokeAnalysisRequests(ctx, workItemIDs)
}

func (as *analysisService) ReexamineAnalysisRequestsBatch(ctx context.Context, workItemIDs []uuid.UUID) error {
	log.Trace().Msgf("Reexamine %d work-item(s) by IDs", len(workItemIDs))
	return as.analysisRepository.IncreaseReexaminationRequestedCount(ctx, workItemIDs)
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

func (as *analysisService) RetransmitResult(ctx context.Context, resultID uuid.UUID) error {
	analysisResult, err := as.analysisRepository.GetAnalysisResultByID(ctx, resultID, true)
	if err != nil {
		return err
	}

	as.manager.SendResultForProcessing(analysisResult)

	return nil
}

func (as *analysisService) RetransmitResultBatches(ctx context.Context, batchIDs []uuid.UUID) error {
	log.Trace().Msgf("Trying to retransmit %d analysis results by ID", len(batchIDs))

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
