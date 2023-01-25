package skeleton

import (
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestInstrumentCache(t *testing.T) {
	instrumentCache := NewInstrumentCache()

	instrument1ID := uuid.New()

	instrument1 := Instrument{
		ID:       instrument1ID,
		Name:     "instrument 1",
		Hostname: "host1",
	}

	instrument2ID := uuid.New()

	instrument2 := Instrument{
		ID:       instrument2ID,
		Name:     "instrument 2",
		Hostname: "host2",
	}

	instrument3ID := uuid.New()

	instrument3 := Instrument{
		ID:       instrument3ID,
		Name:     "instrument 3",
		Hostname: "host3",
	}

	instrumentCache.Set([]Instrument{instrument1, instrument2, instrument3})

	cachedInstrument1, ok := instrumentCache.GetByID(instrument1ID)
	assert.Equal(t, true, ok)
	assert.Equal(t, "instrument 1", cachedInstrument1.Name)
	assert.Equal(t, "host1", cachedInstrument1.Hostname)

	cachedInstrument2, ok := instrumentCache.GetByID(instrument2ID)
	assert.Equal(t, true, ok)
	assert.Equal(t, "instrument 2", cachedInstrument2.Name)
	assert.Equal(t, "host2", cachedInstrument2.Hostname)

	cachedInstrument3, ok := instrumentCache.GetByID(instrument3ID)
	assert.Equal(t, true, ok)
	assert.Equal(t, "instrument 3", cachedInstrument3.Name)
	assert.Equal(t, "host3", cachedInstrument3.Hostname)

	_, ok = instrumentCache.GetByID(uuid.New())
	assert.Equal(t, false, ok)

	cachedInstrument1ByIP, ok := instrumentCache.GetByIP("host1")
	assert.Equal(t, true, ok)
	assert.Equal(t, "instrument 1", cachedInstrument1ByIP.Name)
	assert.Equal(t, instrument1ID, cachedInstrument1ByIP.ID)

	cachedInstrument2ByIP, ok := instrumentCache.GetByIP("host2")
	assert.Equal(t, true, ok)
	assert.Equal(t, "instrument 2", cachedInstrument2ByIP.Name)
	assert.Equal(t, instrument2ID, cachedInstrument2ByIP.ID)

	cachedInstrument3ByIP, ok := instrumentCache.GetByIP("host3")
	assert.Equal(t, true, ok)
	assert.Equal(t, "instrument 3", cachedInstrument3ByIP.Name)
	assert.Equal(t, instrument3ID, cachedInstrument3ByIP.ID)

	_, ok = instrumentCache.GetByIP("notstored")
	assert.Equal(t, false, ok)
}
