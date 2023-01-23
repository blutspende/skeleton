package repository

import (
	"github.com/DRK-Blutspende-BaWueHe/skeleton/consolelog/model"
	"github.com/google/uuid"
	"sync"

	"github.com/rs/zerolog/log"
)

type ConsoleLogRepository interface {
	CreateConsoleLog(entity model.ConsoleLogEntity)
	LoadConsoleLogs(instrumentID uuid.UUID) []model.ConsoleLogEntity
}

type ConsoleLogStorage struct {
	mutex       *sync.Mutex
	consoleLogs []*model.ConsoleLogEntity
	size        int
}

func NewConsoleLogRepository(size int) ConsoleLogRepository {
	log.Trace().Msg("Creating new  console log repository")
	return &ConsoleLogStorage{
		mutex:       &sync.Mutex{},
		consoleLogs: make([]*model.ConsoleLogEntity, 0, size),
		size:        size,
	}
}

func (s *ConsoleLogStorage) CreateConsoleLog(entity model.ConsoleLogEntity) {
	log.Trace().Interface("object", entity).Msg("Saving console log")
	s.store(&entity)
}

func (s *ConsoleLogStorage) LoadConsoleLogs(instrumentID uuid.UUID) []model.ConsoleLogEntity {
	log.Trace().Msg("Loading console logs")
	consoleLogEntities := make([]model.ConsoleLogEntity, 0, s.size)
	consoleLogCount := len(s.consoleLogs)
	for i := 0; i < consoleLogCount-1; i++ {
		if s.consoleLogs[i] != nil {
			if (*s.consoleLogs[i]).InstrumentID == instrumentID {
				consoleLogEntities = append(consoleLogEntities, *s.consoleLogs[i])
			}
		}
	}
	return consoleLogEntities
}

func (s *ConsoleLogStorage) store(dto *model.ConsoleLogEntity) {
	s.mutex.Lock()
	if len(s.consoleLogs) < s.size {
		s.consoleLogs = append(s.consoleLogs, nil)
	} else {
		s.consoleLogs[s.size-1] = nil
	}
	copy(s.consoleLogs[1:s.size], s.consoleLogs)
	s.consoleLogs[0] = dto
	s.mutex.Unlock()
}
