package etly

import (
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"time"
)

func TestDuration_TimeUnit(t *testing.T) {
	d := Duration{Duration: 240, Unit: "milli"}
	duration, err := d.TimeUnit()
	assert.Empty(t, err)
	assert.Equal(t, time.Millisecond, duration)
}

func TestDuration_Get(t *testing.T) {
	{
		d := Duration{Duration: 240, Unit: "milli"}
		duration, err := d.Get()
		assert.Empty(t, err)
		//Inbuilt type duration is int64 and always holds value at nano second level
		assert.Equal(t, 240000000, int(duration))
	}
}

func TestTransferPrintFormat(t *testing.T) {
	const sourceStringToEscape = "<dateFormat:yyyy>/<dateFormat:MM>/<dateFormat:dd>/<dateFormat:HH>"
	const targetStringToEscape = "<dateFormat:HH>"

	transferConfig, err := NewTransferConfigFromURL("file://" + GetCurrentWorkingDir() + "/test/transfer_config3.json")
	assert.Nil(t, err)
	assert.NotNil(t, transferConfig)
	assert.NotNil(t, transferConfig.Transfers[0])


	transfer := transferConfig.Transfers[0]
	name := transfer.Name
	source := transfer.Source.Name
	target := transfer.Target.Name

	assert.True(t, strings.Contains(source, sourceStringToEscape))
	assert.True(t, strings.Contains(target, targetStringToEscape))

	toString := transfer.String();
	assert.True(t, strings.Contains(toString, name))
	assert.True(t, !strings.Contains(toString, sourceStringToEscape))
	assert.True(t, !strings.Contains(toString, targetStringToEscape))
}