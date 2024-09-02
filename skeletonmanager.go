package skeleton

import (
	"context"
	"github.com/blutspende/skeleton/utils"
	"sync"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

type instrumentEventType int

const (
	InstrumentAddedEvent = 1 << iota
	InstrumentUpdatedEvent
	InstrumentAddRetryEvent
)

func (ie instrumentEventType) IsOneOf(event instrumentEventType) bool {
	return event&ie != 0
}

func (ie instrumentEventType) IsExactly(event instrumentEventType) bool {
	return event == ie
}

type InstrumentQueueListener interface {
	ProcessInstrumentEvent(instrumentID uuid.UUID, event instrumentEventType)
}

type Manager interface {
	EnqueueInstrument(id uuid.UUID, event instrumentEventType)
	RegisterInstrumentQueueListener(listener InstrumentQueueListener, events ...instrumentEventType)

	SetCallbackHandler(eventHandler SkeletonCallbackHandlerV1)
	GetCallbackHandler() SkeletonCallbackHandlerV1

	SendAnalysisRequestsForProcessing(analysisRequests []AnalysisRequest)
	GetProcessableAnalysisRequestQueue() *utils.ConcurrentQueue[[]AnalysisRequest]

	SendResultForProcessing(analysisResult AnalysisResult)
	GetResultChan() chan AnalysisResult

	SendControlResultForProcessing(controlResult MappedStandaloneControlResult)
	GetControlResultChan() chan MappedStandaloneControlResult
}

type instrumentEvent struct {
	instrumentID uuid.UUID
	event        instrumentEventType
}

type manager struct {
	resultsChan                         chan AnalysisResult
	controlResultsChan                  chan MappedStandaloneControlResult
	processableAnalysisRequestBatchChan chan []AnalysisRequest
	processableAnalysisRequestQueue     *utils.ConcurrentQueue[[]AnalysisRequest]
	instrumentEventChan                 chan instrumentEvent
	instrumentQueueListeners            map[instrumentEventType][]InstrumentQueueListener
	instrumentQueueListenersMutex       sync.Mutex
	callbackEventHandler                SkeletonCallbackHandlerV1
	callbackEventHandlerMutex           sync.Mutex
}

func NewSkeletonManager(ctx context.Context) Manager {
	skeletonManager := &manager{
		resultsChan:                         make(chan AnalysisResult, 500),
		controlResultsChan:                  make(chan MappedStandaloneControlResult, 500),
		processableAnalysisRequestBatchChan: make(chan []AnalysisRequest, 0),
		processableAnalysisRequestQueue:     utils.NewConcurrentQueue[[]AnalysisRequest](ctx),
		instrumentEventChan:                 make(chan instrumentEvent, 0),
		instrumentQueueListeners:            make(map[instrumentEventType][]InstrumentQueueListener, 0),
	}

	go skeletonManager.listenOnInstruments()

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

func (sm *manager) EnqueueInstrument(id uuid.UUID, event instrumentEventType) {
	sm.instrumentEventChan <- instrumentEvent{
		instrumentID: id,
		event:        event,
	}
}

func (sm *manager) RegisterInstrumentQueueListener(listener InstrumentQueueListener, events ...instrumentEventType) {
	sm.instrumentQueueListenersMutex.Lock()
	defer sm.instrumentQueueListenersMutex.Unlock()
	for _, event := range events {
		if listeners, ok := sm.instrumentQueueListeners[event]; ok {
			sm.instrumentQueueListeners[event] = append(listeners, listener)
		} else {
			sm.instrumentQueueListeners[event] = []InstrumentQueueListener{listener}
		}
	}
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

func (sm *manager) SendControlResultForProcessing(controlResult MappedStandaloneControlResult) {
	sm.controlResultsChan <- controlResult
}

func (sm *manager) GetControlResultChan() chan MappedStandaloneControlResult {
	return sm.controlResultsChan
}

func (sm *manager) listenOnInstruments() {
	for {
		select {
		case instrumentEvent, ok := <-sm.instrumentEventChan:
			{
				if !ok {
					log.Error().Msg("Failed to read from instrument CerberusID channel")
					break
				}
				sm.instrumentQueueListenersMutex.Lock()
				listeners, ok := sm.instrumentQueueListeners[instrumentEvent.event]
				sm.instrumentQueueListenersMutex.Unlock()
				if ok {
					for i := range listeners {
						listeners[i].ProcessInstrumentEvent(instrumentEvent.instrumentID, instrumentEvent.event)
					}
				}
			}
		}
	}
}
