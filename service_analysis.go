package skeleton

import (
	"context"
)

type AnalysisService interface {
	CreateAnalysisRequests(ctx context.Context, requests []AnalysisRequest) ([]AnalysisRequestStatus, error)
}

type analysisService struct {
	analysisRepository AnalysisRepository
}

func (as *analysisService) CreateAnalysisRequests(ctx context.Context, requests []AnalysisRequest) ([]AnalysisRequestStatus, error) {
	return []AnalysisRequestStatus{}, nil
}

func NewAnalysisService(analysisRepository AnalysisRepository) AnalysisService {
	return &analysisService{
		analysisRepository: analysisRepository,
	}
}
