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

type StorageProvider struct {
	Registry map[string]func(config *StorageConfig) (storage.Service, error)
}

func (p *StorageProvider) Get(namespace string) func(config *StorageConfig) (storage.Service, error) {
	return p.Registry[namespace]
}

func init() {
	NewStorageProvider().Registry["gs"] = func(config *StorageConfig) (storage.Service, error) {
		credential := option.WithServiceAccountFile(config.Config)
		return gs.NewService(credential), nil
	}

	NewStorageProvider().Registry["s3"] = func(config *StorageConfig) (storage.Service, error) {
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
}

func NewStorageProvider() *StorageProvider {
	if storageProvider != nil {
		return storageProvider
	}
	storageProvider = &StorageProvider{
		Registry: make(map[string]func(config *StorageConfig) (storage.Service, error)),
	}
	return storageProvider
}
