package etly_test

import (
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/viant/etly"
	"github.com/viant/toolbox"
	"time"
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

var Log1ToLog2 = func(source interface{}) (interface{}, error) {
	log1, casted := source.(*Log1)
	if !casted {
		return nil, fmt.Errorf("Failed to cast source: %T, expected %T", source, &Log1{})
	}
	return &Log2{Key: log1.Id, Value: log1.Name + "/" + log1.Type}, nil
}

func init() {
	etly.NewTransformerRegistry().Register("service_test.Log1ToLog2", Log1ToLog2)
	etly.NewProviderRegistry().Register("service_test.Log1", func() interface{} {
		return &Log1{}
	})
}

func TestService_Run(t *testing.T) {

	var files = []string{etly.GetCurrentWorkingDir() + "test/data/out/1_file1.log",
		etly.GetCurrentWorkingDir() + "test/data/out/0_file2.log",
		etly.GetCurrentWorkingDir() + "test/data/out/meta.json",
	}
	for _, file := range files {
		if toolbox.FileExists(file) {
			os.Remove(file)
		}
		defer os.Remove(file)
	}


	var serverConfigUrl = "file://" + etly.GetCurrentWorkingDir() + "/test/server_config.json"
	serverConfig, err := etly.NewServerConfigFromURL(serverConfigUrl)
	assert.Nil(t, err)

	var transferConfigUrl = "file://" + etly.GetCurrentWorkingDir() + "/test/transfer_config.json"
	transferConfig, err := etly.NewTransferConfigFromURL(transferConfigUrl)
	assert.Nil(t, err)

	s, err := etly.NewService(serverConfig, transferConfig)
	if err != nil {
		log.Fatal(err)
	}
	err = s.Run()

	time.Sleep(1 *time.Second)
	for _, file := range files {
		assert.True(t, toolbox.FileExists(file))
	}
	assert.Nil(t, err)

	response := s.ProcessingStatus("meta")
	assert.Equal(t, "", response.Error)
	assert.Equal(t, 4, response.Status[0].Status.RecordProcessed)

}
