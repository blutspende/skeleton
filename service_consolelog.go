package skeleton

import (
	"context"
	"github.com/google/uuid"
	"time"
)

type ConsoleLogService interface {
	Debug(instrumentID uuid.UUID, messageType string, message string)
	Info(instrumentID uuid.UUID, messageType string, message string)
	Error(instrumentID uuid.UUID, messageType string, message string)
	StartConsoleLogSending(ctx context.Context)
}

type consoleLogService struct {
	cerberusClient         CerberusClient
	consoleLogDTOChan      chan ConsoleLogDTO
	consoleLogFlushSeconds int
}

func NewConsoleLogService(cerberusClient CerberusClient, consoleLogFlushSeconds int) ConsoleLogService {
	return &consoleLogService{
		cerberusClient:         cerberusClient,
		consoleLogDTOChan:      make(chan ConsoleLogDTO, 1024),
		consoleLogFlushSeconds: consoleLogFlushSeconds,
	}
}

func (s *consoleLogService) Debug(instrumentID uuid.UUID, messageType string, message string) {
	s.consoleLogDTOChan <- ConsoleLogDTO{
		InstrumentID: instrumentID,
		CreatedAt:    time.Now().UTC(),
		Level:        Debug,
		Message:      message,
		MessageType:  messageType,
	}
}

func (s *consoleLogService) Info(instrumentID uuid.UUID, messageType string, message string) {
	s.consoleLogDTOChan <- ConsoleLogDTO{
		InstrumentID: instrumentID,
		CreatedAt:    time.Now().UTC(),
		Level:        Info,
		Message:      message,
		MessageType:  messageType,
	}
}

func (s *consoleLogService) Error(instrumentID uuid.UUID, messageType string, message string) {
	s.consoleLogDTOChan <- ConsoleLogDTO{
		InstrumentID: instrumentID,
		CreatedAt:    time.Now().UTC(),
		Level:        Error,
		Message:      message,
		MessageType:  messageType,
	}
}

func (s *consoleLogService) StartConsoleLogSending(ctx context.Context) {
	batchSize := 100
	batch := make([]ConsoleLogDTO, 0, batchSize)
	timeout := time.Duration(s.consoleLogFlushSeconds) * time.Second
	ticker := time.NewTicker(timeout)
	for {
		select {
		case <-ctx.Done():
			ticker.Stop()
			return
		case consoleLogDTO := <-s.consoleLogDTOChan:
			batch = append(batch, consoleLogDTO)
			if len(batch) < batchSize {
				continue
			}
			ticker.Stop()
			s.cerberusClient.SendConsoleLog(batch)
			batch = make([]ConsoleLogDTO, 0, batchSize)
			ticker.Reset(timeout)
		case <-ticker.C:
			if len(batch) == 0 {
				continue
			}
			s.cerberusClient.SendConsoleLog(batch)
			batch = make([]ConsoleLogDTO, 0, batchSize)
		}
	}
}
