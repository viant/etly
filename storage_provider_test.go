package etly

import (
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/viant/toolbox"
)

func TestGetStorageService(t *testing.T) {
	//Get current path to get to mock file
	fileName, _, _ := toolbox.CallerInfo(2)
	currentPath, _ := path.Split(fileName)
	credentialPath := currentPath + "/test/mock_s3.json"
	resource := &Resource{
		Name:           "s3://path",
		CredentialFile: credentialPath,
	}

	service, err := getStorageService(resource)

	assert.Nil(t, err)
	assert.NotNil(t, service)

}
