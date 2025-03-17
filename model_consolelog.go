package skeleton

import (
	"time"

	"github.com/google/uuid"
)

type LogLevel string // @Name LogLevel

const (
	Debug LogLevel = "debug"
	Info  LogLevel = "info"
	Error LogLevel = "error"
)

type ConsoleLogEntity struct {
	InstrumentID uuid.UUID
	CreatedAt    time.Time
	Level        LogLevel
	Message      string
	MessageType  string
}

type ConsoleLogDTO struct {
	InstrumentID uuid.UUID `json:"instrumentId" swaggertype:"string" format:"uuid"`   // The instrument ID
	CreatedAt    time.Time `json:"timestamp" swaggertype:"string" format:"date-time"` // The log timestamp
	Level        LogLevel  `json:"messageCategory"`                                   // The log level
	Message      string    `json:"message"`                                           // The log message
	MessageType  string    `json:"messageType"`                                       // The log message type
} // @Name ConsoleLogDTO
