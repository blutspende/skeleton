package model

import (
	"time"

	"github.com/google/uuid"
)

type LogLevel string // @Name LogLevel

const (
	Debug LogLevel = "DEBUG"
	Info  LogLevel = "INFO"
	Error LogLevel = "ERROR"
)

type ConsoleLogEntity struct {
	InstrumentID uuid.UUID
	CreatedAt    time.Time
	Level        LogLevel
	Message      string
}

type ConsoleLogDTO struct {
	InstrumentID uuid.UUID `json:"instrumentId" swaggertype:"string" format:"uuid"`   // The instrument ID
	CreatedAt    time.Time `json:"createdAt" swaggertype:"string" format:"date-time"` // The log timestamp
	Level        LogLevel  `json:"level"`                                             // The log level
	Message      string    `json:"message"`                                           // The log message
} // @Name ConsoleLogDTO
