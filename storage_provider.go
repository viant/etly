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
	"net/url"
	"fmt"
)

var storageProvider *StorageProvider

const (
	GoogleStorage = "gs"
	AmazonStorage = "s3"
)

type Provide func(credentialFile string) (storage.Service, error)

type StorageProvider struct {
	Registry map[string]Provide
}

func (p *StorageProvider) Get(namespace string) func(credentialFile  string) (storage.Service, error) {
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

func provideGCSStorage(credentialFile  string) (storage.Service, error) {
	credentialOption := option.WithServiceAccountFile(credentialFile)
	return gs.NewService(credentialOption), nil
}

func provideAWSStorage(credentialFile string) (storage.Service, error) {
	if !strings.HasPrefix(credentialFile, "/") {
		dir, _ := filepath.Abs(filepath.Dir(os.Args[0]))
		credentialFile = path.Join(dir, credentialFile)
	}
	s3config := &aws.Config{}
	err := toolbox.LoadConfigFromUrl("file://"+credentialFile, s3config)
	if err != nil {
		return nil, err
	}
	return aws.NewService(s3config), nil
}




func getStorageService(resource *Resource) (storage.Service, error) {
	parsedURL, err := url.Parse(resource.Name)
	if err != nil {
		return nil, err
	}
	service := storage.NewService()
	provider := NewStorageProvider().Get(parsedURL.Scheme)
	if provider != nil {
		storageForSchema, err :=provider(resource.CredentialFile)
		if err != nil {
			return nil, fmt.Errorf("Failed to get storage for url %v", resource.Name)
		}
		service.Register(parsedURL.Scheme, storageForSchema)

	}
	return service, nil
}