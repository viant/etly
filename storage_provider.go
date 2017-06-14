package etly

import (
	"github.com/viant/toolbox"
	"github.com/viant/toolbox/storage"
	"github.com/viant/toolbox/storage/aws"
	"github.com/viant/toolbox/storage/gs"
	"google.golang.org/api/option"
	"os"
	"path"
	"path/filepath"
	"strings"
)

var storageProvider *StorageProvider

const (
	GoogleStorage = "gs"
	AmazonStorage = "s3"
)

type Provide func(config *StorageConfig) (storage.Service, error)

type StorageProvider struct {
	Registry map[string]Provide
}

func (p *StorageProvider) Get(namespace string) func(config *StorageConfig) (storage.Service, error) {
	return p.Registry[namespace]
}

func init() {
	NewStorageProvider().Registry[GoogleStorage] = provideGCSStorage
	NewStorageProvider().Registry[AmazonStorage] = provideAWSStorage
}

func NewStorageProvider() *StorageProvider {
	if storageProvider != nil {
		return storageProvider
	}
	storageProvider = &StorageProvider{
		Registry: make(map[string]Provide),
	}
	return storageProvider
}

func provideGCSStorage(config *StorageConfig) (storage.Service, error) {
	credential := option.WithServiceAccountFile(config.Config)
	return gs.NewService(credential), nil
}

func provideAWSStorage(config *StorageConfig) (storage.Service, error) {
	var file = config.Config
	if !strings.HasPrefix(file, "/") {
		dir, _ := filepath.Abs(filepath.Dir(os.Args[0]))
		file = path.Join(dir, file)
	}
	s3config := &aws.Config{}
	err := toolbox.LoadConfigFromUrl("file://"+file, s3config)
	if err != nil {
		return nil, err
	}
	return aws.NewService(s3config), nil
}
