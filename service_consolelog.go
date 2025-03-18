package skeleton

import (
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
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

func (s *consoleLogService) Debug(instrumentID uuid.UUID, messageType string, message string) {
	err := s.cerberusClient.SendConsoleLog(instrumentID, Debug, messageType, message)
	if err != nil {
		log.Error().Err(err).Str("InstrumentId", instrumentID.String()).Msg("Error sending console log")
	}
}

func (s *consoleLogService) Info(instrumentID uuid.UUID, messageType string, message string) {
	err := s.cerberusClient.SendConsoleLog(instrumentID, Info, messageType, message)
	if err != nil {
		log.Error().Err(err).Str("InstrumentId", instrumentID.String()).Msg("Error sending console log")
	}
}

func (s *consoleLogService) Error(instrumentID uuid.UUID, messageType string, message string) {
	err := s.cerberusClient.SendConsoleLog(instrumentID, Error, messageType, message)
	if err != nil {
		log.Error().Err(err).Str("InstrumentId", instrumentID.String()).Msg("Error sending console log")
	}
}
