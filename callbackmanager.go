package skeleton

import "sync"

type CallbackManager interface {
	SetCallbackHandler(eventHandler SkeletonCallbackHandlerV1)
	GetCallbackHandler() SkeletonCallbackHandlerV1

}

type callbackManager struct {
	callbackEventHandler      SkeletonCallbackHandlerV1
	callbackEventHandlerMutex sync.Mutex
}

func NewCallbackManager() CallbackManager {
	return &callbackManager{}
}

func (sm *callbackManager) SetCallbackHandler(eventHandler SkeletonCallbackHandlerV1) {
	sm.callbackEventHandlerMutex.Lock()
	defer sm.callbackEventHandlerMutex.Unlock()
	sm.callbackEventHandler = eventHandler
}
func (sm *callbackManager) GetCallbackHandler() SkeletonCallbackHandlerV1 {
	sm.callbackEventHandlerMutex.Lock()
	defer sm.callbackEventHandlerMutex.Unlock()
	return sm.callbackEventHandler
}
