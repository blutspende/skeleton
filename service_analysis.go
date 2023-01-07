package skeleton

import (
	"context"
)

type AnalysisService interface {
	CreateAnalysisRequests(ctx context.Context, requests []AnalysisRequest) ([]AnalysisRequestStatus, error)
}

type analysisService struct {
	//analysisRequestRepository
}

func (as *analysisService) CreateAnalysisRequests(ctx context.Context, requests []AnalysisRequest) ([]AnalysisRequestStatus, error) {

	return []AnalysisRequestStatus{}, nil
}

func NewAnalysisService() AnalysisService {
	return &analysisService{}
}
