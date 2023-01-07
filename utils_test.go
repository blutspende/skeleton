package skeleton_test


import (
	"github.com/DRK-Blutspende-BaWueHe/skeleton"
	"testing"

	assert "github.com/go-playground/assert/v2"
)

func TestStandardizeDecimalValueNormalization(t *testing.T) {

	assert.Equal(t, "1456", skeleton.StandardizeUSDecimalValue("1,456"))
	assert.Equal(t, "1456.15", skeleton.StandardizeUSDecimalValue("1,456.15"))
	assert.Equal(t, "1231456.15", skeleton.StandardizeUSDecimalValue("1,231,456.15"))
	assert.Equal(t, "1456.151231233", skeleton.StandardizeUSDecimalValue("1,456.151231233"))
	assert.Equal(t, "0.4234", skeleton.StandardizeUSDecimalValue(".4234"))       // no preceeding dots without 0.
	assert.Equal(t, "5424234", skeleton.StandardizeUSDecimalValue("5,424,234.")) // do not leave trailing dots

	assert.Equal(t, "12345678.15", skeleton.StandardizeEUDecimalValue("12345678,15"))   // stays as is
	assert.Equal(t, "12345678", skeleton.StandardizeEUDecimalValue("12.345.678"))       // european writing ...
	assert.Equal(t, "12345678.15", skeleton.StandardizeEUDecimalValue("12.345.678,15")) // eu with decimals
	assert.Equal(t, "0.123456", skeleton.StandardizeEUDecimalValue(",123456"))          // eu only decimals
	assert.Equal(t, "56789", skeleton.StandardizeEUDecimalValue("56789,"))              // eu only decimals
}