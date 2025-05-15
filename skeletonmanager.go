package skeleton

import (
	"context"
	"github.com/blutspende/skeleton/utils"
	"github.com/google/uuid"
	"sync"

	"github.com/rs/zerolog/log"
)

type Manager interface {
	SetCallbackHandler(eventHandler SkeletonCallbackHandlerV1)
	GetCallbackHandler() SkeletonCallbackHandlerV1

	SendAnalysisRequestsForProcessing(analysisRequests []AnalysisRequest)
	GetProcessableAnalysisRequestQueue() *utils.ConcurrentQueue[[]AnalysisRequest]

	SendResultForProcessing(analysisResult AnalysisResult)
	GetResultChan() chan AnalysisResult

	SendAnalyteMappingsToValidateControlResults(analyteMappingIds []uuid.UUID)
	GetControlValidationChan() chan []uuid.UUID

	SendControlResultIdsToAnalysisResultStatusRecalculation(controlResultIds []uuid.UUID)
	GetAnalysisResultStatusRecalculationChan() chan []uuid.UUID
}

type manager struct {
	resultsChan                         chan AnalysisResult
	processableAnalysisRequestBatchChan chan []AnalysisRequest
	processableAnalysisRequestQueue     *utils.ConcurrentQueue[[]AnalysisRequest]
	analyteMappingIdBatchChan           chan []uuid.UUID
	controlResultIdBatchChan            chan []uuid.UUID
	callbackEventHandler                SkeletonCallbackHandlerV1
	callbackEventHandlerMutex           sync.Mutex
}

func NewSkeletonManager(ctx context.Context) Manager {
	skeletonManager := &manager{
		resultsChan:                         make(chan AnalysisResult, 500),
		processableAnalysisRequestBatchChan: make(chan []AnalysisRequest),
		processableAnalysisRequestQueue:     utils.NewConcurrentQueue[[]AnalysisRequest](ctx),
		analyteMappingIdBatchChan:           make(chan []uuid.UUID, 10),
		controlResultIdBatchChan:            make(chan []uuid.UUID, 10),
	}

	return skeletonManager
}

func (sm *manager) SetCallbackHandler(eventHandler SkeletonCallbackHandlerV1) {
	sm.callbackEventHandlerMutex.Lock()
	defer sm.callbackEventHandlerMutex.Unlock()
	sm.callbackEventHandler = eventHandler
}

func (sm *manager) GetCallbackHandler() SkeletonCallbackHandlerV1 {
	sm.callbackEventHandlerMutex.Lock()
	defer sm.callbackEventHandlerMutex.Unlock()
	return sm.callbackEventHandler
}

func (sm *manager) SendAnalysisRequestsForProcessing(analysisRequests []AnalysisRequest) {
	log.Trace().Msgf("Sending %d analysis request(s) for processing", len(analysisRequests))

	sm.processableAnalysisRequestQueue.Enqueue(analysisRequests)
}

func (sm *manager) GetProcessableAnalysisRequestQueue() *utils.ConcurrentQueue[[]AnalysisRequest] {
	return sm.processableAnalysisRequestQueue
}

func (sm *manager) SendResultForProcessing(analysisResult AnalysisResult) {
	sm.resultsChan <- analysisResult
}

func (sm *manager) GetResultChan() chan AnalysisResult {
	return sm.resultsChan
}

func (sm *manager) SendAnalyteMappingsToValidateControlResults(analyteMappingIds []uuid.UUID) {
	sm.analyteMappingIdBatchChan <- analyteMappingIds
}

func (sm *manager) GetControlValidationChan() chan []uuid.UUID {
	return sm.analyteMappingIdBatchChan
}

func (sm *manager) SendControlResultIdsToAnalysisResultStatusRecalculation(controlResultIds []uuid.UUID) {
	sm.controlResultIdBatchChan <- controlResultIds
}

func (sm *manager) GetAnalysisResultStatusRecalculationChan() chan []uuid.UUID {
	return sm.controlResultIdBatchChan
}
