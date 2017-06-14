package etly

import "github.com/viant/toolbox/storage"

type StorageObjectTransfer struct {
	Transfer       *Transfer
	StorageObjects []storage.Object
}
