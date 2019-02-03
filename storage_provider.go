package etly

import (
	"github.com/viant/toolbox/storage"
	_ "github.com/viant/toolbox/storage/s3"
	_ "github.com/viant/toolbox/storage/gs"
	_ "github.com/viant/toolbox/storage/scp"
)

func getStorageService(resource *Resource) (storage.Service, error) {
	return storage.NewServiceForURL(resource.Name, resource.CredentialFile)
}
