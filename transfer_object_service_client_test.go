package etly

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTransferObjectServiceClient_new(t *testing.T) {
	config := ServerConfig{
		TimeOut: &Duration{Duration: 6, Unit: "min"},
	}
	client := newTransferObjectServiceClient(config.Cluster, config.TimeOut)
	assert.NotNil(t, client)
}

func TestTransferObjectServiceClient_new_panic(t *testing.T) {
	// Should panic due to unsupported unit
	config := ServerConfig{
		TimeOut: &Duration{Duration: 6, Unit: "mins"}, //Unsupported unit
	}
	assert.Panics(t, func() { newTransferObjectServiceClient(config.Cluster, config.TimeOut) })
}
