package skeleton

import (
	"context"
	"github.com/blutspende/skeleton/db"
	"github.com/blutspende/skeleton/utils"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCreateAnalysisRequests(t *testing.T) {
	mockManager := &mockManager{}
	analysisRequests := []AnalysisRequest{
		{WorkItemID: uuid.MustParse("6cdc3aa0-a024-4c51-8d24-8aa12d489f41")},
		{WorkItemID: uuid.MustParse("92a2ba34-d891-4a1b-89fb-e0c4d717f729")},
	}
	analysisService := NewAnalysisService(&extendedMockAnalysisRepo{}, nil, nil, mockManager)
	analysisRequestStatuses, err := analysisService.CreateAnalysisRequests(context.TODO(), analysisRequests)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(analysisRequestStatuses))
	assert.Nil(t, analysisRequestStatuses[0].Error)
	assert.Nil(t, analysisRequestStatuses[1].Error)
	assert.Equal(t, 2, len(mockManager.AnalysisRequestsSentForProcessing))
	assert.Equal(t, uuid.MustParse("6cdc3aa0-a024-4c51-8d24-8aa12d489f41"), mockManager.AnalysisRequestsSentForProcessing[0].WorkItemID)
	assert.Equal(t, uuid.MustParse("92a2ba34-d891-4a1b-89fb-e0c4d717f729"), mockManager.AnalysisRequestsSentForProcessing[1].WorkItemID)
}

func TestCreateAnalysisRequestsWithError(t *testing.T) {
	mockManager := &mockManager{}
	analysisRequests := []AnalysisRequest{
		{WorkItemID: uuid.MustParse("6cdc3aa0-a024-4c51-8d24-8aa12d489f41")},
		{WorkItemID: uuid.MustParse("92a2ba34-d891-4a1b-89fb-e0c4d717f729")},
		{WorkItemID: uuid.MustParse("88b87019-ddcc-4d4b-bc04-9e213680e0db")},
		{WorkItemID: uuid.MustParse("c0dbcfb6-6a90-4ab6-bcab-0cfbec4abd06")},
	}
	analysisService := NewAnalysisService(&extendedMockAnalysisRepo{}, nil, nil, mockManager)
	analysisRequestStatuses, err := analysisService.CreateAnalysisRequests(context.TODO(), analysisRequests)
	assert.NotNil(t, err)
	assert.Equal(t, ErrAnalysisRequestWithMatchingWorkItemIdFound, err)
	assert.Equal(t, 4, len(analysisRequestStatuses))
	assert.Nil(t, analysisRequestStatuses[0].Error)
	assert.Nil(t, analysisRequestStatuses[1].Error)
	assert.Nil(t, analysisRequestStatuses[2].Error)
	assert.NotNil(t, analysisRequestStatuses[3].Error)
	assert.Equal(t, ErrAnalysisRequestWithMatchingWorkItemIdFound, analysisRequestStatuses[3].Error)
	assert.Equal(t, 3, len(mockManager.AnalysisRequestsSentForProcessing))
	assert.Equal(t, uuid.MustParse("6cdc3aa0-a024-4c51-8d24-8aa12d489f41"), mockManager.AnalysisRequestsSentForProcessing[0].WorkItemID)
	assert.Equal(t, uuid.MustParse("92a2ba34-d891-4a1b-89fb-e0c4d717f729"), mockManager.AnalysisRequestsSentForProcessing[1].WorkItemID)
	assert.Equal(t, uuid.MustParse("88b87019-ddcc-4d4b-bc04-9e213680e0db"), mockManager.AnalysisRequestsSentForProcessing[2].WorkItemID)
}

type mockManager struct {
	AnalysisRequestsSentForProcessing []AnalysisRequest
}

func (m *mockManager) SetCallbackHandler(eventHandler SkeletonCallbackHandlerV1) {

}
func (m *mockManager) GetCallbackHandler() SkeletonCallbackHandlerV1 {
	return nil
}
func (m *mockManager) SendAnalysisRequestsForProcessing(analysisRequests []AnalysisRequest) {
	m.AnalysisRequestsSentForProcessing = append(m.AnalysisRequestsSentForProcessing, analysisRequests...)
}
func (m *mockManager) GetProcessableAnalysisRequestQueue() *utils.ConcurrentQueue[[]AnalysisRequest] {
	return nil
}
func (m *mockManager) SendResultForProcessing(analysisResult AnalysisResult) {

}
func (m *mockManager) GetResultChan() chan AnalysisResult {
	return nil
}

type extendedMockAnalysisRepo struct {
	analysisRepositoryMock
}

func (r *extendedMockAnalysisRepo) CreateAnalysisRequestsBatch(ctx context.Context, analysisRequests []AnalysisRequest) ([]uuid.UUID, []uuid.UUID, error) {
	workItemIDs := make([]uuid.UUID, 0)
	for i := range analysisRequests {
		workItemIDs = append(workItemIDs, analysisRequests[i].WorkItemID)
	}
	if len(workItemIDs) < 3 {
		return nil, workItemIDs, nil
	}
	return nil, workItemIDs[:len(workItemIDs)-1], nil
}

func (r *extendedMockAnalysisRepo) WithTransaction(tx db.DbConnector) AnalysisRepository {
	return r
}
