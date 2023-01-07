package services

import (
	"context"
	v1 "skeleton/v1"
)

type AnalysisService interface {
	CreateAnalysisRequests(ctx context.Context, requests []v1.AnalysisRequestV1) ([]v1.AnalysisRequestStatusV1, error)
}

type analysisService struct {
	//analysisRequestRepository
}

func (as *analysisService) CreateAnalysisRequests(ctx context.Context, requests []v1.AnalysisRequestV1) ([]v1.AnalysisRequestStatusV1, error) {

	return []v1.AnalysisRequestStatusV1{}, nil
}

func NewAnalysisService() AnalysisService {
	return &analysisService{}
}
