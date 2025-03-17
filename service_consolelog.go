package skeleton

import (
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"time"
)

type ConsoleLogService interface {
	Debug(instrumentID uuid.UUID, messageType string, message string)
	Info(instrumentID uuid.UUID, messageType string, message string)
	Error(instrumentID uuid.UUID, messageType string, message string)
}

type consoleLogService struct {
	cerberusClient CerberusClient
}

func NewConsoleLogService(cerberusClient CerberusClient) ConsoleLogService {
	return &consoleLogService{
		cerberusClient: cerberusClient,
	}
}

func (s *consoleLogService) createConsoleLog(level LogLevel, instrumentID uuid.UUID, messageType string, message string) {
	consoleLog := ConsoleLogDTO{
		InstrumentID: instrumentID,
		Level:        level,
		CreatedAt:    time.Now().UTC(),
		Message:      message,
		MessageType:  messageType,
	}

	err := s.cerberusClient.SendConsoleLog(instrumentID, consoleLog)
	if err != nil {
		log.Error().Err(err).Str("InstrumentId", instrumentID.String()).Msg("Error sending console log")
	}
}

func (s *consoleLogService) Debug(instrumentID uuid.UUID, messageType string, message string) {
	s.createConsoleLog(Debug, instrumentID, messageType, message)
}

func (s *consoleLogService) Info(instrumentID uuid.UUID, messageType string, message string) {
	s.createConsoleLog(Info, instrumentID, messageType, message)
}

func (s *consoleLogService) Error(instrumentID uuid.UUID, messageType string, message string) {
	s.createConsoleLog(Error, instrumentID, messageType, message)
}
