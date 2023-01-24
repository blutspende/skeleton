package service

import (
	"github.com/DRK-Blutspende-BaWueHe/skeleton/consolelog/model"
	"github.com/DRK-Blutspende-BaWueHe/skeleton/consolelog/repository"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMessages(t *testing.T) {
	consoleLogRepository := repository.NewConsoleLogRepository(5)
	consoleLogService := NewConsoleLogService(consoleLogRepository)

	instrumentID := uuid.MustParse("845b73b5-c92d-4797-93a6-0165b3366404")

	consoleLogService.Info(instrumentID, "Info message")
	consoleLogService.Debug(instrumentID, "Debug message")
	consoleLogService.Error(instrumentID, "Error message")

	messages := consoleLogService.GetConsoleLogs(instrumentID)
	assert.Equal(t, 3, len(messages))

	assert.Equal(t, model.Error, messages[0].Level)
	assert.Equal(t, "Error message", messages[0].Message)
	assert.Equal(t, model.Debug, messages[1].Level)
	assert.Equal(t, "Debug message", messages[1].Message)
	assert.Equal(t, model.Info, messages[2].Level)
	assert.Equal(t, "Info message", messages[2].Message)

	messages = consoleLogService.GetConsoleLogs(uuid.Nil)
	assert.Equal(t, 0, len(messages))
}
