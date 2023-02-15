package server

import (
	"context"
	"github.com/gin-contrib/sse"
)

type SSEResultStatus int

const (
	SSEDelivered SSEResultStatus = iota
	SSEError
	SSEUndelivered
)

type SSEClient struct {
	Context    context.Context
	EventChan  chan sse.Event
	ResultChan chan SSEResult
}

type SSEResult struct {
	Result SSEResultStatus
	Event  sse.Event
	Error  error
}

type SSEClientChan chan sse.Event
