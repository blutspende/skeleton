package skeleton

import (
	"context"
	"github.com/google/uuid"
)

type AnalysisService interface {
	CreateAnalysisRequests(ctx context.Context, requests []AnalysisRequest) ([]AnalysisRequestStatus, error)
	GetAnalysisRequestsInfo(ctx context.Context, instrumentID uuid.UUID, pageable Pageable) ([]AnalysisRequestInfo, int, error)
}

type analysisService struct {
	analysisRepository AnalysisRepository
}

func NewAnalysisService(analysisRepository AnalysisRepository) AnalysisService {
	return &analysisService{
		analysisRepository: analysisRepository,
	}
}

func (as *analysisService) CreateAnalysisRequests(ctx context.Context, requests []AnalysisRequest) ([]AnalysisRequestStatus, error) {
	_, err := as.analysisRepository.CreateAnalysisRequestsBatch(ctx, requests)
	if err != nil {
		return nil, err
	}

	return []AnalysisRequestStatus{}, nil
}

func (as *analysisService) GetAnalysisRequestsInfo(ctx context.Context, instrumentID uuid.UUID, pageable Pageable) ([]AnalysisRequestInfo, int, error) {
	requestInfoList, totalCount, err := as.analysisRepository.GetAnalysisRequestsInfo(ctx, instrumentID, pageable)
	if err != nil {
		return []AnalysisRequestInfo{}, 0, err
	}

	return requestInfoList, totalCount, nil
}
