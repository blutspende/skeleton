package service

import (
	"github.com/DRK-Blutspende-BaWueHe/skeleton/consolelog/model"
	"github.com/DRK-Blutspende-BaWueHe/skeleton/consolelog/repository"
	"github.com/google/uuid"
	"time"

	"github.com/rs/zerolog/log"
)

type ConsoleLogService interface {
	Debug(instrumentID uuid.UUID, message string)
	Info(instrumentID uuid.UUID, message string)
	Error(instrumentID uuid.UUID, message string)
	GetConsoleLogs(instrumentID uuid.UUID) []model.ConsoleLogDTO
}

type consoleLogService struct {
	repository repository.ConsoleLogRepository
}

func NewConsoleLogService(repository repository.ConsoleLogRepository) ConsoleLogService {
	log.Trace().Msg("Creating new console log service")
	return &consoleLogService{
		repository: repository,
	}
}

func (s *consoleLogService) createConsoleLog(level model.LogLevel, instrumentID uuid.UUID, message string) {
	log.Trace().Interface("level", level).Interface("message", message).Msg("Creating console log")

	newConsoleLogEntity := model.ConsoleLogEntity{
		InstrumentID: instrumentID,
		Level:        level,
		CreatedAt:    time.Now().UTC(),
		Message:      message,
	}

	s.repository.CreateConsoleLog(newConsoleLogEntity)
}

func (s *consoleLogService) Debug(instrumentID uuid.UUID, message string) {
	s.createConsoleLog(model.Debug, instrumentID, message)
}

func (s *consoleLogService) Info(instrumentID uuid.UUID, message string) {
	s.createConsoleLog(model.Info, instrumentID, message)
}

func (s *consoleLogService) Error(instrumentID uuid.UUID, message string) {
	s.createConsoleLog(model.Error, instrumentID, message)
}

func (s *consoleLogService) GetConsoleLogs(instrumentID uuid.UUID) []model.ConsoleLogDTO {
	log.Trace().Msg("Getting console logs")

	loadedConsoleLogEntities := s.repository.LoadConsoleLogs(instrumentID)

	entityCount := len(loadedConsoleLogEntities)
	consoleLogDTOs := make([]model.ConsoleLogDTO, entityCount)
	for i := 0; i < entityCount; i++ {
		consoleLogEntity := loadedConsoleLogEntities[i]
		consoleLogDTOs[i] = model.ConsoleLogDTO{
			Level:     consoleLogEntity.Level,
			CreatedAt: consoleLogEntity.CreatedAt,
			Message:   consoleLogEntity.Message,
		}
	}

	return consoleLogDTOs
}
