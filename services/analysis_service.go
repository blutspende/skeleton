package services

import (
	"astm/skeletonapi"
	"context"
)

type AnalysisService interface {
	CreateAnalysisRequests(ctx context.Context, requests []skeletonapi.AnalysisRequestV1) ([]skeletonapi.AnalysisRequestStatusV1, error)
}

type analysisService struct {
	//analysisRequestRepository
}

func (as *analysisService) CreateAnalysisRequests(ctx context.Context, requests []skeletonapi.AnalysisRequestV1) ([]skeletonapi.AnalysisRequestStatusV1, error) {

	return []skeletonapi.AnalysisRequestStatusV1{}, nil
}

func NewAnalysisService() AnalysisService {
	return &analysisService{}
}
