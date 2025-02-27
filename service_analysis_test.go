package skeleton

import (
	"context"
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
	analysisService := NewAnalysisService(&analysisRepositoryMock{}, nil, nil, mockManager)
	err := analysisService.CreateAnalysisRequests(context.TODO(), analysisRequests)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(mockManager.AnalysisRequestsSentForProcessing))
	assert.Equal(t, uuid.MustParse("6cdc3aa0-a024-4c51-8d24-8aa12d489f41"), mockManager.AnalysisRequestsSentForProcessing[0].WorkItemID)
	assert.Equal(t, uuid.MustParse("92a2ba34-d891-4a1b-89fb-e0c4d717f729"), mockManager.AnalysisRequestsSentForProcessing[1].WorkItemID)
}

func TestCreateAnalysisRequestDuplicates(t *testing.T) {
	mockManager := &mockManager{}
	analysisRequests := []AnalysisRequest{
		{ID: uuid.MustParse("5e095a05-ede9-4fa4-aa6d-05d8514aa3b6"), WorkItemID: uuid.MustParse("6cdc3aa0-a024-4c51-8d24-8aa12d489f41")},
		{ID: uuid.MustParse("07831faf-69f9-4c32-b994-603bd774a516"), WorkItemID: uuid.MustParse("6cdc3aa0-a024-4c51-8d24-8aa12d489f41")},
		{ID: uuid.MustParse("66c96c9b-cf3a-4916-adbe-3ae0db005391"), WorkItemID: uuid.MustParse("88b87019-ddcc-4d4b-bc04-9e213680e0db")},
		{ID: uuid.MustParse("83a80ca2-54e9-4833-8cd1-e8bb5202b33e"), WorkItemID: uuid.MustParse("c0dbcfb6-6a90-4ab6-bcab-0cfbec4abd06")},
	}
	analysisService := NewAnalysisService(&analysisRepositoryMock{}, nil, nil, mockManager)
	err := analysisService.CreateAnalysisRequests(context.TODO(), analysisRequests)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(mockManager.AnalysisRequestsSentForProcessing))
	assert.Equal(t, uuid.MustParse("6cdc3aa0-a024-4c51-8d24-8aa12d489f41"), mockManager.AnalysisRequestsSentForProcessing[0].WorkItemID)
	assert.Equal(t, uuid.MustParse("5e095a05-ede9-4fa4-aa6d-05d8514aa3b6"), mockManager.AnalysisRequestsSentForProcessing[0].ID)

	assert.Equal(t, uuid.MustParse("88b87019-ddcc-4d4b-bc04-9e213680e0db"), mockManager.AnalysisRequestsSentForProcessing[1].WorkItemID)
	assert.Equal(t, uuid.MustParse("66c96c9b-cf3a-4916-adbe-3ae0db005391"), mockManager.AnalysisRequestsSentForProcessing[1].ID)

	assert.Equal(t, uuid.MustParse("c0dbcfb6-6a90-4ab6-bcab-0cfbec4abd06"), mockManager.AnalysisRequestsSentForProcessing[2].WorkItemID)
	assert.Equal(t, uuid.MustParse("83a80ca2-54e9-4833-8cd1-e8bb5202b33e"), mockManager.AnalysisRequestsSentForProcessing[2].ID)
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
