package etly

import (
	"fmt"
	"sync"
)

var variableProviderRegistry *VariableProviderRegistry

type VariableProvider func(source interface{}) string

// VariableProviderRegistry contains map of variable providers which can be access by multiple goroutines
type VariableProviderRegistry struct {
	lock     *sync.Mutex
	registry map[string]VariableProvider
}

func (r *VariableProviderRegistry) Register(name string, provider VariableProvider) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.registry[name] = provider
}

func (r *VariableProviderRegistry) Get(name string) (VariableProvider, error) {
	if result, found := r.registry[name]; found {
		return result, nil
	}
	return nil, fmt.Errorf("Failed to lookp variable provider: %v", name)

}

// NewVariableProviderRegistry initializes threadsafe VariableProvider map
func NewVariableProviderRegistry() *VariableProviderRegistry {
	if variableProviderRegistry != nil {
		return variableProviderRegistry
	}
	variableProviderRegistry = &VariableProviderRegistry{
		lock:     new(sync.Mutex),
		registry: make(map[string]VariableProvider),
	}
	return variableProviderRegistry
}
