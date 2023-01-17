package skeleton

import (
	"context"
	"github.com/rs/zerolog/log"
)

type AnalysisService interface {
	CreateAnalysisRequests(ctx context.Context, requests []AnalysisRequest) ([]AnalysisRequestStatus, error)
}

type analysisService struct {
	analysisRepository AnalysisRepository
}

func (as *analysisService) CreateAnalysisRequests(ctx context.Context, requests []AnalysisRequest) ([]AnalysisRequestStatus, error) {
	createdRequestIDs, err := as.analysisRepository.CreateAnalysisRequestsBatch(ctx, requests)
	if err != nil {
		return nil, err
	}

	log.Debug().Interface("savedAnalysisRequestIDs", createdRequestIDs).Msg("Saved analysis request IDs")

	return []AnalysisRequestStatus{}, nil
}

func NewAnalysisService(analysisRepository AnalysisRepository) AnalysisService {
	return &analysisService{
		analysisRepository: analysisRepository,
	}
}
