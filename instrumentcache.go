package skeleton

import (
	"sync"

	"github.com/google/uuid"
)

type InstrumentCache interface {
	Invalidate()
	Set([]Instrument)
	GetByID(id uuid.UUID) (Instrument, bool)
	GetByIP(ip string) (Instrument, bool)
	GetAll() []Instrument
}

type instrumentCache struct {
	instruments       []Instrument
	instrumentMapByID map[uuid.UUID]*Instrument
	instrumentMapByIP map[string]*Instrument
	mutex             sync.Mutex
}

func NewInstrumentCache() InstrumentCache {
	return &instrumentCache{}
}

func (ic *instrumentCache) Invalidate() {
	ic.mutex.Lock()
	defer ic.mutex.Unlock()
	if ic.instruments == nil {
		return
	}
	for _, instrument := range ic.instruments {
		ic.instrumentMapByID[instrument.ID] = nil
		ic.instrumentMapByIP[instrument.Hostname] = nil
	}
	ic.instruments = nil
}

func (ic *instrumentCache) Set(instruments []Instrument) {
	ic.Invalidate()

	ic.mutex.Lock()
	defer ic.mutex.Unlock()
	ic.instruments = instruments
	ic.instrumentMapByID = make(map[uuid.UUID]*Instrument, len(instruments))
	ic.instrumentMapByIP = make(map[string]*Instrument, len(instruments))
	for _, instrument := range instruments {
		ic.instrumentMapByID[instrument.ID] = &instrument
		ic.instrumentMapByIP[instrument.Hostname] = &instrument
	}
}

func (ic *instrumentCache) GetByID(id uuid.UUID) (Instrument, bool) {
	ic.mutex.Lock()
	defer ic.mutex.Unlock()
	if ic.instruments == nil {
		return Instrument{}, false
	}
	instrument, ok := ic.instrumentMapByID[id]
	return *instrument, ok
}

func (ic *instrumentCache) GetByIP(ip string) (Instrument, bool) {
	ic.mutex.Lock()
	defer ic.mutex.Unlock()
	if ic.instruments == nil {
		return Instrument{}, false
	}
	instrument, ok := ic.instrumentMapByIP[ip]
	return *instrument, ok
}

func (ic *instrumentCache) GetAll() []Instrument {
	ic.mutex.Lock()
	defer ic.mutex.Unlock()
	if ic.instruments == nil {
		return []Instrument{}
	}
	return ic.instruments
}
