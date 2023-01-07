package skeleton

import (
	"context"
)

type AnalysisService interface {
	CreateAnalysisRequests(ctx context.Context, requests []AnalysisRequestV1) ([]AnalysisRequestStatusV1, error)
}

type analysisService struct {
	//analysisRequestRepository
}

func (as *analysisService) CreateAnalysisRequests(ctx context.Context, requests []AnalysisRequestV1) ([]AnalysisRequestStatusV1, error) {

	return []AnalysisRequestStatusV1{}, nil
}

func NewAnalysisService() AnalysisService {
	return &analysisService{}
}
