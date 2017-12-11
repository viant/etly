package etly

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"time"
)

func TestDuration_TimeUnit(t *testing.T) {
	d := Duration {Duration:240, Unit:"milli"}
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