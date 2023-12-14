package server

import (
	"context"
	"github.com/blutspende/skeleton/consolelog/model"
	"github.com/gin-contrib/sse"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"io"
)

type ConsoleLogSSE struct {
	ID            string
	EventCategory string
	Target        string
	TargetType    string
	Data          interface{}
}

type ConsoleLogSSEServer struct {
	clientListener    ConsoleLogSSEClientListener
	MessageChan       chan model.ConsoleLogDTO
	NewClientsChan    chan *SSEClient
	ClosedClientsChan chan *SSEClient
	TotalClients      map[uuid.UUID]map[*SSEClient]interface{}
	ResultChan        chan SSEResult
}

type ConsoleLogSSEClientListener interface {
	OnSSENewClient(newClient *SSEClient)
	OnSSEClientClosing(client *SSEClient)
	OnSSEClientClosed()
	OnSSESend(message model.ConsoleLogDTO, client *SSEClient) error
	OnSSESendingCompleted(result SSEResult)
}

func NewConsoleLogSSEClient(context context.Context) *SSEClient {
	return &SSEClient{
		Context:   context,
		EventChan: make(chan sse.Event),
	}
}

func NewConsoleLogSSEServer(listener ConsoleLogSSEClientListener) *ConsoleLogSSEServer {
	event := &ConsoleLogSSEServer{
		clientListener:    listener,
		MessageChan:       make(chan model.ConsoleLogDTO),
		NewClientsChan:    make(chan *SSEClient),
		ClosedClientsChan: make(chan *SSEClient),
		TotalClients:      make(map[uuid.UUID]map[*SSEClient]interface{}),
		ResultChan:        make(chan SSEResult),
	}

	go event.listen()
	go event.listenOnResultCallback()

	return event
}

func (e *ConsoleLogSSEServer) Send(messages ...model.ConsoleLogDTO) {
	for _, message := range messages {
		e.MessageChan <- message
	}
}

func (e *ConsoleLogSSEServer) ServeHTTP() gin.HandlerFunc {
	return func(c *gin.Context) {
		client := NewConsoleLogSSEClient(c)

		e.NewClientsChan <- client

		defer func() {
			e.ClosedClientsChan <- client
		}()

		c.Stream(func(w io.Writer) bool {
			event, ok := <-client.EventChan
			if ok {
				select {
				case isClientDisconnected := <-w.(gin.ResponseWriter).CloseNotify():
					if isClientDisconnected {
						e.ResultChan <- SSEResult{
							Result: SSEUndelivered,
							Event:  event,
						}
					}
					return false
				default:
					c.Render(-1, event)
					e.ResultChan <- SSEResult{
						Result: SSEDelivered,
						Event:  event,
					}
				}
			}
			return ok
		})
	}
}

func (e *ConsoleLogSSEServer) listen() {
	for {
		select {
		case client := <-e.NewClientsChan:
			if ginCtx, ok := client.Context.(*gin.Context); ok {
				instrumentID, err := uuid.Parse(ginCtx.Param("instrumentId"))
				if err == nil {
					if e.clientListener != nil {
						e.clientListener.OnSSENewClient(client)
					}

					if _, ok := e.TotalClients[instrumentID]; !ok {
						e.TotalClients[instrumentID] = make(map[*SSEClient]interface{})
					}

					e.TotalClients[instrumentID][client] = struct{}{}
					log.Debug().Msgf("Client added... %s has %d registered clients", instrumentID.String(), len(e.TotalClients[instrumentID]))
				}
			}
		case client := <-e.ClosedClientsChan:
			if ginCtx, ok := client.Context.(*gin.Context); ok {
				instrumentID, err := uuid.Parse(ginCtx.Param("instrumentId"))
				if err == nil {
					if e.clientListener != nil {
						e.clientListener.OnSSEClientClosing(client)
					}
					delete(e.TotalClients[instrumentID], client)
					close(client.EventChan)
					if e.clientListener != nil {
						e.clientListener.OnSSEClientClosed()
					}
					log.Debug().Msgf("Removed client... %s has %d registered clients", instrumentID.String(), len(e.TotalClients[instrumentID]))
				}
			}
		case message := <-e.MessageChan:
			for sseClient := range e.TotalClients[message.InstrumentID] {
				if e.clientListener != nil {
					e.doListenerMessageSending(message, sseClient)
				} else {
					e.doDefaultMessageSending(message, sseClient)
				}
			}
		}
	}
}

func (e *ConsoleLogSSEServer) listenOnResultCallback() {
	for {
		result := <-e.ResultChan

		if e.clientListener != nil {
			e.clientListener.OnSSESendingCompleted(result)
		}
	}
}

func (e *ConsoleLogSSEServer) doListenerMessageSending(event model.ConsoleLogDTO, client *SSEClient) {
	err := e.clientListener.OnSSESend(event, client)
	if err != nil {
		log.Error().Err(err).
			Interface("client", *client).
			Interface("data", event).
			Msg("Failed to send notification")

		e.ResultChan <- SSEResult{
			Event: sse.Event{
				Data: event,
			},
			Error: err,
		}
	}
}

func (e *ConsoleLogSSEServer) doDefaultMessageSending(message model.ConsoleLogDTO, client *SSEClient) {
	client.EventChan <- sse.Event{
		Id:   uuid.NewString(),
		Data: message,
	}
}
