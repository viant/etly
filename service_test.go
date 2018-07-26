package etly

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/viant/toolbox"
	"context"
)

type Log1 struct {
	Id   int
	Name string
	Type string
}

type Log2 struct {
	Key   int
	Value string
}

type AppLog1 struct {
	AppId   string `json:"APP_ID"`
	Name    string `json:"NAME"`
	URL     string `json:"URL"`
	Payload string
}

type AppLog2 struct {
	Payload string
	AppId   int
	Name    string
	URL     string
}

func (l *AppLog1) SetPayload(payload string) {
	l.Payload = payload
}

var Log1ToLog2 = func(source interface{}) (interface{}, error) {
	log1, casted := source.(*Log1)
	if !casted {
		return nil, fmt.Errorf("Failed to cast source: %T, expected %T", source, &Log1{})
	}
	return &Log2{Key: log1.Id, Value: log1.Name + "/" + log1.Type}, nil
}

var AppLog1ToLog2 = func(source interface{}) (interface{}, error) {
	sourceLog, casted := source.(*AppLog1)
	if !casted {
		return nil, fmt.Errorf("failed to cast source: %T, expected %T", source, &Log1{})
	}
	return &AppLog2{
		AppId:   toolbox.AsInt(sourceLog.AppId),
		Name:    sourceLog.Name,
		URL:     sourceLog.URL,
		Payload: sourceLog.Payload,
	}, nil
}

func init() {
	NewTransformerRegistry().Register("service_test.Log1ToLog2", Log1ToLog2)
	NewTransformerRegistry().Register("service_test.AppLog1ToLog2", AppLog1ToLog2)

	NewProviderRegistry().Register("service_test.Log1", func() interface{} {
		return &Log1{}
	})

	NewProviderRegistry().Register("AppLog1.log", func() interface{} {
		return &AppLog1{}
	})

}

func TestService_RunStorageToStorage(t *testing.T) {

	var files = []string{GetCurrentWorkingDir() + "test/data/out/1_file1.log",
		GetCurrentWorkingDir() + "test/data/out/0_file2.log",
		GetCurrentWorkingDir() + "test/data/out/meta.json",
	}
	for _, file := range files {
		if toolbox.FileExists(file) {
			os.Remove(file)
		}
		defer os.Remove(file)
	}

	var serverConfigUrl = "file://" + GetCurrentWorkingDir() + "/test/server_config.json"
	serverConfig, err := NewServerConfigFromURL(serverConfigUrl)
	assert.Nil(t, err)
	assert.Equal(t, 300, serverConfig.TimeOut.Duration, "serverConfig.TimeOut.Duration")
	assert.Equal(t, "milli", serverConfig.TimeOut.Unit, "serverConfig.TimeOut.Unit")

	var transferConfigUrl = "file://" + GetCurrentWorkingDir() + "/test/transfer_config1.json"
	transferConfig, err := NewTransferConfigFromURL(transferConfigUrl)
	assert.Nil(t, err)
	assert.Equal(t, 300, transferConfig.Transfers[0].TimeOut.Duration, "transferConfig.TimeOut.Duration")
	assert.Equal(t, "milli", transferConfig.Transfers[0].TimeOut.Unit, "transferConfig.TimeOut.Unit")

	s, err := NewService(serverConfig, transferConfig)
	if err != nil {
		log.Fatal(err)
	}
	err = s.Start()

	//err = s.Run()

	time.Sleep(10 * time.Second)
	for _, file := range files {
		assert.True(t, toolbox.FileExists(file))
	}
	assert.Nil(t, err)

	response := s.ProcessingStatus("meta")
	assert.Equal(t, "", response.Error)
	assert.Equal(t, 4, response.Status[0].Status.RecordProcessed)
	assert.Equal(t, 1, len(response.Status[0].Errors))

	assert.True(t, strings.Contains(response.Status[0].Errors[0].Error, "failed to decode json (1 times): unexpected EOF, {\"werwe:"))

}

func TestService_RunStorageToDatastore(t *testing.T) {
	var serverConfigUrl = "file://" + GetCurrentWorkingDir() + "/test/server_config.json"
	serverConfig, err := NewServerConfigFromURL(serverConfigUrl)
	var transferConfigUrl = "file://" + GetCurrentWorkingDir() + "/test/transfer_config2.json"
	transferConfig, err := NewTransferConfigFromURL(transferConfigUrl)
	assert.Nil(t, err)
	assert.Equal(t, 300, transferConfig.Transfers[0].TimeOut.Duration, "transferConfig.TimeOut.Duration")
	assert.Equal(t, "milli", transferConfig.Transfers[0].TimeOut.Unit, "transferConfig.TimeOut.Unit")

	var files = []string{
		GetCurrentWorkingDir() + "test/ds/out/app-0-0.log",
	}
	for _, file := range files {
		if toolbox.FileExists(file) {
			os.Remove(file)
		}
		//defer os.Remove(file)
	}

	ctx := context.Background()
	s, err := NewServiceWithContext(ctx, serverConfig, transferConfig)
	if err != nil {
		log.Fatal(err)
	}
	err = s.Run()

	if assert.Nil(t, err) {
		time.Sleep(5 * time.Second)
		response := s.ProcessingStatus("meta")
		assert.Equal(t, "", response.Error)

		time.Sleep(5 * time.Second)
		for _, file := range files {
			if assert.True(t, toolbox.FileExists(file)) {
				content, _ := ioutil.ReadFile(file)
				lines := strings.Split(string(content), "\n")
				assert.Equal(t, 4, len(lines))
				assert.True(t, strings.Contains(lines[0], "\"AppId\":1"), lines[0])
				assert.True(t, strings.Contains(lines[1], "\"AppId\":2"), lines[1])

			}
		}
	}
}

func TestService_GetTasksByStatus(t *testing.T) {

	var files = []string{GetCurrentWorkingDir() + "test/data/out/1_file1.log",
		GetCurrentWorkingDir() + "test/data/out/0_file2.log",
		GetCurrentWorkingDir() + "test/data/out/meta.json",
	}
	for _, file := range files {
		if toolbox.FileExists(file) {
			os.Remove(file)
		}
		defer os.Remove(file)
	}

	var serverConfigUrl = "file://" + GetCurrentWorkingDir() + "/test/server_config.json"
	serverConfig, _ := NewServerConfigFromURL(serverConfigUrl)
	var transferConfigUrl = "file://" + GetCurrentWorkingDir() + "/test/transfer_config1.json"
	transferConfig, _ := NewTransferConfigFromURL(transferConfigUrl)
	ctx := context.Background()
	s, e := NewServerWithContext(ctx, serverConfig, transferConfig)
	assert.Nil(t, e)
	go func() {
		s.Start()
	}()
	time.Sleep(2 * time.Second)
	response := &TaskListResponse{}
	err := toolbox.RouteToService("get", "http://127.0.0.1:8090/etly/tasks?status=DONE", nil, &response)
	assert.Nil(t, err)
	assert.NotNil(t, response)
	assert.NotNil(t, response.Tasks)
	assert.Condition(t, func() bool { return len(response.Tasks) >= 1 })
}
