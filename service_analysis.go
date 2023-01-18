package skeleton

import (
	"context"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

type AnalysisService interface {
	CreateAnalysisRequests(ctx context.Context, analysisRequests []AnalysisRequest) ([]AnalysisRequestStatus, error)
	ProcessAnalysisRequests(ctx context.Context, analysisRequests []AnalysisRequest) error
	GetAnalysisRequestsInfo(ctx context.Context, instrumentID uuid.UUID, pageable Pageable) ([]AnalysisRequestInfo, int, error)
}

type analysisService struct {
	analysisRepository AnalysisRepository
	manager            Manager
}

func NewAnalysisService(analysisRepository AnalysisRepository, manager Manager) AnalysisService {
	return &analysisService{
		analysisRepository: analysisRepository,
		manager:            manager,
	}
}

func (as *analysisService) CreateAnalysisRequests(ctx context.Context, analysisRequests []AnalysisRequest) ([]AnalysisRequestStatus, error) {
	_, err := as.analysisRepository.CreateAnalysisRequestsBatch(ctx, analysisRequests)
	if err != nil {
		return nil, err
	}

	as.manager.SendAnalysisRequestsForProcessing(analysisRequests)

	// Todo give back correct response
	return []AnalysisRequestStatus{}, nil
}

func (as *analysisService) ProcessAnalysisRequests(ctx context.Context, analysisRequests []AnalysisRequest) error {
	for _, request := range analysisRequests {
		analysisResults, err := as.analysisRepository.GetAnalysisResultsBySampleCodeAndAnalyteID(ctx, request.SampleCode, request.AnalyteID)
		if err != nil {
			log.Debug().Err(err).Str("requestID", request.ID.String()).Msg("Failed to load analysis results for the request")
			return err
		}

		for i := range analysisResults {
			as.manager.SendResultForProcessing(analysisResults[i])
		}
	}

	return nil
}

func (as *analysisService) GetAnalysisRequestsInfo(ctx context.Context, instrumentID uuid.UUID, pageable Pageable) ([]AnalysisRequestInfo, int, error) {
	requestInfoList, totalCount, err := as.analysisRepository.GetAnalysisRequestsInfo(ctx, instrumentID, pageable)
	if err != nil {
		return []AnalysisRequestInfo{}, 0, err
	}

	return requestInfoList, totalCount, nil
}
