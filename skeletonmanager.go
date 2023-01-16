package skeleton

import (
	"sync"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

type instrumentEvent int

const (
	InstrumentAddedEvent = 1 << iota
	InstrumentUpdatedEvent
)

func (ie instrumentEvent) IsOneOf(event instrumentEvent) bool {
	return event&ie != 0
}

func (ie instrumentEvent) IsExactly(event instrumentEvent) bool {
	return event == ie
}

type InstrumentQueueListener interface {
	ProcessInstrument(instrumentID uuid.UUID, event instrumentEvent)
}

type Manager interface {
	EnqueueInstrument(id uuid.UUID, event instrumentEvent)
	RegisterInstrumentQueueListener(listener InstrumentQueueListener, events ...instrumentEvent)

	SetCallbackHandler(eventHandler SkeletonCallbackHandlerV1)
	GetCallbackHandler() SkeletonCallbackHandlerV1
}

type manager struct {
	instrumentIDChan              chan uuid.UUID
	instrumentQueueListeners      map[instrumentEvent][]InstrumentQueueListener
	instrumentQueueListenersMutex sync.Mutex
	callbackEventHandler          SkeletonCallbackHandlerV1
	callbackEventHandlerMutex     sync.Mutex
}

func NewCallbackManager() Manager {
	skeletonManager := &manager{
		instrumentIDChan:         make(chan uuid.UUID, 0),
		instrumentQueueListeners: make(map[instrumentEvent][]InstrumentQueueListener, 0),
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

func (sm *manager) EnqueueInstrument(id uuid.UUID, event instrumentEvent) {
	sm.instrumentIDChan <- id
}

func (sm *manager) RegisterInstrumentQueueListener(listener InstrumentQueueListener, events ...instrumentEvent) {
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

func (sm *manager) listenOnInstruments() {
	for {
		select {
		case id, ok := <-sm.instrumentIDChan:
			{
				if !ok {
					log.Error().Msg("Failed to read from instrument ID channel")
					break
				}
				sm.instrumentQueueListenersMutex.Lock()
				for event, listeners := range sm.instrumentQueueListeners {
					for i := range listeners {
						listeners[i].ProcessInstrument(id, event)
					}
				}
				sm.instrumentQueueListenersMutex.Unlock()
			}
		}
	}
}
