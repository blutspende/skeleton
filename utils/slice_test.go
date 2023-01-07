package utils_test

import (
	"astm/skeleton/utils"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSliceContains(t *testing.T) {

	fruits := []string{"cherry", "banana", "pineapple",
		"peach", "pea", "ananas", "elderberry", "blackberry", "strawberry"}

	assert.True(t, utils.SliceContains("apple", fruits))

	assert.False(t, utils.SliceContains("handgranade", fruits))
}
