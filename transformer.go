package etly

import "sync"

var transformerRegistry *TransformerRegistry

type Transformer func(soure interface{}) (interface{}, error)

// TransformerRegistry contains map of tranformers which can be access by multiple goroutines
type TransformerRegistry struct {
	lock     *sync.Mutex
	registry map[string]Transformer
}

func (r *TransformerRegistry) Register(name string, transformer Transformer) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.registry[name] = transformer
}

// NewTransformerRegistry initializes threadsafe Tranformer map
func NewTransformerRegistry() *TransformerRegistry {
	if transformerRegistry != nil {
		return transformerRegistry
	}
	transformerRegistry = &TransformerRegistry{
		lock:     new(sync.Mutex),
		registry: make(map[string]Transformer),
	}
	return transformerRegistry
}
