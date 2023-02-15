package service

import (
	"github.com/DRK-Blutspende-BaWueHe/skeleton/consolelog/model"
	"github.com/DRK-Blutspende-BaWueHe/skeleton/server"
	"github.com/gin-contrib/sse"
)

type ConsoleLogClientListener struct {
}

func NewConsoleLogSSEClientListener() server.ConsoleLogSSEClientListener {
	return &ConsoleLogClientListener{}
}

func (l *ConsoleLogClientListener) OnSSENewClient(newClient *server.SSEClient) {
}

func (l *ConsoleLogClientListener) OnSSEClientClosing(client *server.SSEClient) {
}

func (l *ConsoleLogClientListener) OnSSEClientClosed() {
}

func (l *ConsoleLogClientListener) OnSSESend(message model.ConsoleLogDTO, client *server.SSEClient) error {
	client.EventChan <- sse.Event{
		Data: message,
	}

	return nil
}

func (l *ConsoleLogClientListener) OnSSESendingCompleted(result server.SSEResult) {
}
