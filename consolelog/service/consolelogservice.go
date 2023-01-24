package service

import (
	"github.com/DRK-Blutspende-BaWueHe/skeleton/consolelog/model"
	"github.com/DRK-Blutspende-BaWueHe/skeleton/consolelog/repository"
	"github.com/google/uuid"
	"time"
)

type ConsoleLogService interface {
	Debug(instrumentID uuid.UUID, messageType string, message string)
	Info(instrumentID uuid.UUID, messageType string, message string)
	Error(instrumentID uuid.UUID, messageType string, message string)
	GetConsoleLogs(instrumentID uuid.UUID) []model.ConsoleLogDTO
}

type consoleLogService struct {
	repository repository.ConsoleLogRepository
}

func NewConsoleLogService(repository repository.ConsoleLogRepository) ConsoleLogService {
	return &consoleLogService{
		repository: repository,
	}
}

func (s *consoleLogService) createConsoleLog(level model.LogLevel, instrumentID uuid.UUID, messageType string, message string) {
	newConsoleLogEntity := model.ConsoleLogEntity{
		InstrumentID: instrumentID,
		Level:        level,
		CreatedAt:    time.Now().UTC(),
		Message:      message,
		MessageType:  messageType,
	}

	s.repository.CreateConsoleLog(newConsoleLogEntity)
}

func (s *consoleLogService) Debug(instrumentID uuid.UUID, messageType string, message string) {
	s.createConsoleLog(model.Debug, instrumentID, messageType, message)
}

func (s *consoleLogService) Info(instrumentID uuid.UUID, messageType string, message string) {
	s.createConsoleLog(model.Info, instrumentID, messageType, message)
}

func (s *consoleLogService) Error(instrumentID uuid.UUID, messageType string, message string) {
	s.createConsoleLog(model.Error, instrumentID, messageType, message)
}

func (s *consoleLogService) GetConsoleLogs(instrumentID uuid.UUID) []model.ConsoleLogDTO {
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
