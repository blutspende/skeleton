package utils_test

import (
	"github.com/DRK-Blutspende-BaWueHe/skeleton/utils"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSliceContains(t *testing.T) {

	fruits := []string{"cherry", "banana", "pineapple", "apple",
		"peach", "pea", "ananas", "elderberry", "blackberry", "strawberry"}

	assert.True(t, utils.SliceContains("apple", fruits))

	assert.False(t, utils.SliceContains("handgranade", fruits))
}
