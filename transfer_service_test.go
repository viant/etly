package etly

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_GetTransferForTimeWindow_InDays(t *testing.T) {
	transferConfig, err := NewTransferConfigFromURL("file://" + GetCurrentWorkingDir() + "/test/test_transfer_config.json")
	assert.Nil(t, err)
	assert.NotNil(t, transferConfig)
	assert.NotNil(t, transferConfig.Transfers[0])
	transferTask := NewTransferTask(transferConfig.Transfers[0])
	serverConfig, err := NewServerConfigFromURL("file://" + GetCurrentWorkingDir() + "/test/server_config.json")
	s, err := NewService(serverConfig, transferConfig)
	transfers, err := s.transferService.getTransferForTimeWindow(transferTask.Transfer)
	assert.Nil(t, err)
	assert.NotNil(t, transfers)
	assert.Equal(t, 2, len(transfers))
}

func Test_GetTransferForTimeWindow_InHours(t *testing.T) {
	transferConfig, err := NewTransferConfigFromURL("file://" + GetCurrentWorkingDir() + "/test/test_transfer_config.json")
	assert.Nil(t, err)
	assert.NotNil(t, transferConfig)
	assert.NotNil(t, transferConfig.Transfers[0])

	//Overriding config for this test case
	transfer := transferConfig.Transfers[0]
	transfer.TimeWindow = &Duration{Duration: 24, Unit: "hour"}

	transferTask := NewTransferTask(transfer)
	serverConfig, err := NewServerConfigFromURL("file://" + GetCurrentWorkingDir() + "/test/server_config.json")
	s, err := NewService(serverConfig, transferConfig)
	transfers, err := s.transferService.getTransferForTimeWindow(transferTask.Transfer)
	assert.Nil(t, err)
	assert.NotNil(t, transfers)
	assert.Equal(t, 24, len(transfers))
}
