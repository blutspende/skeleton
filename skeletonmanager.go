package skeleton

import (
	"context"
	"github.com/blutspende/skeleton/utils"
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
}

type manager struct {
	resultsChan                         chan AnalysisResult
	processableAnalysisRequestBatchChan chan []AnalysisRequest
	processableAnalysisRequestQueue     *utils.ConcurrentQueue[[]AnalysisRequest]
	instrumentQueueListenersMutex       sync.Mutex
	callbackEventHandler                SkeletonCallbackHandlerV1
	callbackEventHandlerMutex           sync.Mutex
}

func NewSkeletonManager(ctx context.Context) Manager {
	skeletonManager := &manager{
		resultsChan:                         make(chan AnalysisResult, 500),
		processableAnalysisRequestBatchChan: make(chan []AnalysisRequest, 0),
		processableAnalysisRequestQueue:     utils.NewConcurrentQueue[[]AnalysisRequest](ctx),
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
