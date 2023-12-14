package service

import (
	"github.com/blutspende/skeleton/consolelog/model"
	"github.com/blutspende/skeleton/consolelog/repository"
	"github.com/blutspende/skeleton/server"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMessages(t *testing.T) {
	consoleLogRepository := repository.NewConsoleLogRepository(5)
	consoleLogSSEServer := server.NewConsoleLogSSEServer(nil)
	consoleLogService := NewConsoleLogService(consoleLogRepository, consoleLogSSEServer)

	instrumentID := uuid.MustParse("845b73b5-c92d-4797-93a6-0165b3366404")

	consoleLogService.Info(instrumentID, "[GENERAL]", "Info message")
	consoleLogService.Debug(instrumentID, "[GENERAL]", "Debug message")
	consoleLogService.Error(instrumentID, "[GENERAL]", "Error message")

	messages := consoleLogService.GetConsoleLogs(instrumentID)
	assert.Equal(t, 3, len(messages))

	assert.Equal(t, model.Error, messages[0].Level)
	assert.Equal(t, "Error message", messages[0].Message)
	assert.Equal(t, "[GENERAL]", messages[0].MessageType)
	assert.Equal(t, model.Debug, messages[1].Level)
	assert.Equal(t, "Debug message", messages[1].Message)
	assert.Equal(t, "[GENERAL]", messages[1].MessageType)
	assert.Equal(t, model.Info, messages[2].Level)
	assert.Equal(t, "Info message", messages[2].Message)
	assert.Equal(t, "[GENERAL]", messages[2].MessageType)

	messages = consoleLogService.GetConsoleLogs(uuid.Nil)
	assert.Equal(t, 0, len(messages))
}
