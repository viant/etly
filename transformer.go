package etly

import "sync"

var transformerRegistry *TransformerRegistry
var transformerRegistryMux = &sync.Mutex{}

type Transformer func(soure interface{}) (interface{}, error)
type TransformerRegistry struct {
	registry map[string]Transformer
}

func (r *TransformerRegistry) Register(name string, transformer Transformer) {
	r.registry[name] = transformer
}

func NewTransformerRegistry() *TransformerRegistry {
	if transformerRegistry != nil {
		return transformerRegistry
	}
	transformerRegistryMux.Lock()
	defer transformerRegistryMux.Unlock()
	if transformerRegistry != nil {
		return transformerRegistry
	}
	transformerRegistry = &TransformerRegistry{
		make(map[string]Transformer),
	}
	return transformerRegistry
}
