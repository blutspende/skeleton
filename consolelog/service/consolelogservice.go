package service

import (
	"context"
	"encoding/json"
	"github.com/blutspende/skeleton/consolelog/model"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
	"time"
)

type ConsoleLogService interface {
	Debug(instrumentID uuid.UUID, messageType string, message string)
	Info(instrumentID uuid.UUID, messageType string, message string)
	Error(instrumentID uuid.UUID, messageType string, message string)
}

type consoleLogService struct {
	redisClient *redis.Client
}

func NewConsoleLogService(redisUrl string) ConsoleLogService {
	client := redis.NewClient(&redis.Options{
		Addr: redisUrl,
	})
	return &consoleLogService{
		redisClient: client,
	}
}

func (s *consoleLogService) createConsoleLog(level model.LogLevel, instrumentID uuid.UUID, messageType string, message string) {
	consoleLog := model.ConsoleLogDTO{
		InstrumentID: instrumentID,
		Level:        level,
		CreatedAt:    time.Now().UTC(),
		Message:      message,
		MessageType:  messageType,
	}

	logData, err := json.Marshal(consoleLog)
	if err != nil {
		log.Error().Err(err).Msg("Error marshalling log")
		return
	}

	s.redisClient.Publish(context.Background(), "logs_channel", logData)
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
