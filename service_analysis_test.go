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
	analysisService := NewAnalysisService(&mockAnalysisRepository{}, nil, nil, mockManager)
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
	analysisService := NewAnalysisService(&mockAnalysisRepository{}, nil, nil, mockManager)
	analysisRequestStatuses, err := analysisService.CreateAnalysisRequests(context.TODO(), analysisRequests)
	assert.NotNil(t, err)
	assert.Equal(t, ErrAnalysisRequestWithMatchingWorkItemIdFound, err)
	assert.Equal(t, 4, len(analysisRequestStatuses))
	assert.Nil(t, analysisRequestStatuses[0].Error)
	assert.Nil(t, analysisRequestStatuses[1].Error)
	assert.Nil(t, analysisRequestStatuses[2].Error)
	assert.NotNil(t, analysisRequestStatuses[3].Error)
	assert.Equal(t, ErrAnalysisRequestWithMatchingWorkItemIdFound, analysisRequestStatuses[3].Error)
	assert.Equal(t, 4, len(mockManager.AnalysisRequestsSentForProcessing))
	assert.Equal(t, uuid.MustParse("6cdc3aa0-a024-4c51-8d24-8aa12d489f41"), mockManager.AnalysisRequestsSentForProcessing[0].WorkItemID)
	assert.Equal(t, uuid.MustParse("92a2ba34-d891-4a1b-89fb-e0c4d717f729"), mockManager.AnalysisRequestsSentForProcessing[1].WorkItemID)
	assert.Equal(t, uuid.MustParse("88b87019-ddcc-4d4b-bc04-9e213680e0db"), mockManager.AnalysisRequestsSentForProcessing[2].WorkItemID)
	assert.Equal(t, uuid.MustParse("c0dbcfb6-6a90-4ab6-bcab-0cfbec4abd06"), mockManager.AnalysisRequestsSentForProcessing[3].WorkItemID)
}

type mockManager struct {
	AnalysisRequestsSentForProcessing []AnalysisRequest
}

func (m *mockManager) EnqueueInstrument(id uuid.UUID, event instrumentEventType) {

}
func (m *mockManager) RegisterInstrumentQueueListener(listener InstrumentQueueListener, events ...instrumentEventType) {

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

type mockAnalysisRepository struct{}

func (r *mockAnalysisRepository) CreateAnalysisRequestsBatch(ctx context.Context, analysisRequests []AnalysisRequest) ([]uuid.UUID, []uuid.UUID, error) {
	workItemIDs := make([]uuid.UUID, 0)
	for i := range analysisRequests {
		workItemIDs = append(workItemIDs, analysisRequests[i].WorkItemID)
	}
	if len(workItemIDs) < 3 {
		return nil, workItemIDs, nil
	}
	return nil, workItemIDs[:len(workItemIDs)-1], nil
}
func (r *mockAnalysisRepository) GetAnalysisRequestsBySampleCodeAndAnalyteID(ctx context.Context, sampleCodes string, analyteID uuid.UUID) ([]AnalysisRequest, error) {
	return nil, nil

}
func (r *mockAnalysisRepository) GetAnalysisRequestsBySampleCodes(ctx context.Context, sampleCodes []string, allowResending bool) (map[string][]AnalysisRequest, error) {
	return nil, nil
}
func (r *mockAnalysisRepository) GetAnalysisRequestsInfo(ctx context.Context, instrumentID uuid.UUID, filter Filter) ([]AnalysisRequestInfo, int, error) {
	return nil, 0, nil
}
func (r *mockAnalysisRepository) GetAnalysisRequestExtraValuesByAnalysisRequestID(ctx context.Context, analysisRequestID uuid.UUID) (map[string]string, error) {
	return nil, nil
}
func (r *mockAnalysisRepository) GetAnalysisResultsInfo(ctx context.Context, instrumentID uuid.UUID, filter Filter) ([]AnalysisResultInfo, int, error) {
	return nil, 0, nil
}
func (r *mockAnalysisRepository) GetAnalysisBatches(ctx context.Context, instrumentID uuid.UUID, filter Filter) ([]AnalysisBatch, int, error) {
	return nil, 0, nil
}
func (r *mockAnalysisRepository) CreateSubjectsBatch(ctx context.Context, subjectInfosByAnalysisRequestID map[uuid.UUID]SubjectInfo) (map[uuid.UUID]uuid.UUID, error) {
	return nil, nil
}
func (r *mockAnalysisRepository) GetSubjectsByAnalysisRequestIDs(ctx context.Context, analysisRequestIDs []uuid.UUID) (map[uuid.UUID]SubjectInfo, error) {
	return nil, nil
}
func (r *mockAnalysisRepository) GetAnalysisRequestsByWorkItemIDs(ctx context.Context, workItemIds []uuid.UUID) ([]AnalysisRequest, error) {
	return nil, nil
}
func (r *mockAnalysisRepository) RevokeAnalysisRequests(ctx context.Context, workItemIds []uuid.UUID) error {
	return nil
}
func (r *mockAnalysisRepository) DeleteAnalysisRequestExtraValues(ctx context.Context, workItemIDs []uuid.UUID) error {
	return nil
}
func (r *mockAnalysisRepository) IncreaseReexaminationRequestedCount(ctx context.Context, workItemIDs []uuid.UUID) error {
	return nil
}
func (r *mockAnalysisRepository) IncreaseSentToInstrumentCounter(ctx context.Context, analysisRequestIDs []uuid.UUID) error {
	return nil
}
func (r *mockAnalysisRepository) SaveAnalysisRequestsInstrumentTransmissions(ctx context.Context, analysisRequestIDs []uuid.UUID, instrumentID uuid.UUID) error {
	return nil
}
func (r *mockAnalysisRepository) CreateAnalysisResultsBatch(ctx context.Context, analysisResults []AnalysisResult) ([]AnalysisResult, error) {
	return nil, nil
}
func (r *mockAnalysisRepository) GetAnalysisResultsBySampleCodeAndAnalyteID(ctx context.Context, sampleCode string, analyteID uuid.UUID) ([]AnalysisResult, error) {
	return nil, nil
}
func (r *mockAnalysisRepository) GetAnalysisResultByID(ctx context.Context, id uuid.UUID, allowDeletedAnalyteMapping bool) (AnalysisResult, error) {
	return AnalysisResult{}, nil
}
func (r *mockAnalysisRepository) GetAnalysisResultsByIDs(ctx context.Context, ids []uuid.UUID) ([]AnalysisResult, error) {
	return nil, nil
}
func (r *mockAnalysisRepository) GetAnalysisResultsByBatchIDs(ctx context.Context, batchIDs []uuid.UUID) ([]AnalysisResult, error) {
	return nil, nil
}
func (r *mockAnalysisRepository) GetAnalysisResultsByBatchIDsMapped(ctx context.Context, batchIDs []uuid.UUID) (map[uuid.UUID][]AnalysisResultInfo, error) {
	return nil, nil

}
func (r *mockAnalysisRepository) GetAnalysisResultQueueItems(ctx context.Context) ([]CerberusQueueItem, error) {
	return nil, nil
}
func (r *mockAnalysisRepository) UpdateAnalysisResultQueueItemStatus(ctx context.Context, queueItem CerberusQueueItem) error {
	return nil
}
func (r *mockAnalysisRepository) CreateAnalysisResultQueueItem(ctx context.Context, analysisResults []AnalysisResult) (uuid.UUID, error) {
	return uuid.UUID{}, nil
}
func (r *mockAnalysisRepository) SaveImages(ctx context.Context, images []imageDAO) ([]uuid.UUID, error) {
	return nil, nil
}
func (r *mockAnalysisRepository) GetStuckImageIDsForDEA(ctx context.Context) ([]uuid.UUID, error) {
	return nil, nil
}
func (r *mockAnalysisRepository) GetStuckImageIDsForCerberus(ctx context.Context) ([]uuid.UUID, error) {
	return nil, nil
}
func (r *mockAnalysisRepository) GetImagesForDEAUploadByIDs(ctx context.Context, ids []uuid.UUID) ([]imageDAO, error) {
	return nil, nil
}
func (r *mockAnalysisRepository) GetImagesForCerberusSyncByIDs(ctx context.Context, ids []uuid.UUID) ([]cerberusImageDAO, error) {
	return nil, nil

}
func (r *mockAnalysisRepository) SaveDEAImageID(ctx context.Context, imageID, deaImageID uuid.UUID) error {
	return nil
}
func (r *mockAnalysisRepository) IncreaseImageUploadRetryCount(ctx context.Context, imageID uuid.UUID, error string) error {
	return nil
}
func (r *mockAnalysisRepository) MarkImagesAsSyncedToCerberus(ctx context.Context, ids []uuid.UUID) error {
	return nil
}
func (r *mockAnalysisRepository) GetUnprocessedAnalysisRequests(ctx context.Context) ([]AnalysisRequest, error) {
	return nil, nil
}
func (r *mockAnalysisRepository) GetUnprocessedAnalysisResultIDs(ctx context.Context) ([]uuid.UUID, error) {
	return nil, nil
}
func (r *mockAnalysisRepository) MarkAnalysisRequestsAsProcessed(ctx context.Context, analysisRequestIDs []uuid.UUID) error {
	return nil
}
func (r *mockAnalysisRepository) MarkAnalysisResultsAsProcessed(ctx context.Context, analysisRequestIDs []uuid.UUID) error {
	return nil
}
func (r *mockAnalysisRepository) CreateTransaction() (db.DbConnector, error) {
	return nil, nil
}
func (r *mockAnalysisRepository) WithTransaction(tx db.DbConnector) AnalysisRepository {
	return nil
}
