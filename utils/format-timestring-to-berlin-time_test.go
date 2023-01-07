package utils

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFormatTimeStringToBerlinTime(t *testing.T) {

	_, err := time.LoadLocation(string("Europe/Berlin"))
	assert.Nil(t, err, "Timezone is not available in this container")
}
