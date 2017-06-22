package etly

import (
	"sync"

	"github.com/viant/toolbox"
)

var filterRegistry *FilterRegistry
var filterRegistryMux = &sync.Mutex{}

type FilterRegistry struct {
	registry map[string]toolbox.Predicate
}

func (r *FilterRegistry) Register(name string, predicate toolbox.Predicate) {
	r.registry[name] = predicate
}

func NewFilterRegistry() *FilterRegistry {
	filterRegistryMux.Lock()
	defer filterRegistryMux.Unlock()
	if filterRegistry != nil {
		return filterRegistry
	}
	filterRegistry = &FilterRegistry{
		make(map[string]toolbox.Predicate),
	}
	return filterRegistry
}
