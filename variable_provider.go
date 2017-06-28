package etly

import (
	"sync"
	"fmt"
)


type VariableProvider func(source interface{}) string

var variableProviderRegistry *VariableProviderRegistry
var variableProviderRegistryMux = &sync.Mutex{}



type VariableProviderRegistry struct {
	registry map[string]VariableProvider
}

func (r *VariableProviderRegistry) Register(name string, provider VariableProvider) {
	r.registry[name] = provider
}



func (r *VariableProviderRegistry) Get(name string) (VariableProvider, error) {
	if result, found :=  r.registry[name];found {
		return result, nil
	}
	return nil, fmt.Errorf("Failed to lookp variable provider: %v", name)


}

func NewVariableProviderRegistry() *VariableProviderRegistry {
	if variableProviderRegistry != nil {
		return variableProviderRegistry
	}
	variableProviderRegistryMux.Lock()
	defer variableProviderRegistryMux.Unlock()
	if variableProviderRegistry != nil {
		return variableProviderRegistry
	}
	variableProviderRegistry = &VariableProviderRegistry{
		make(map[string]VariableProvider),
	}
	return variableProviderRegistry
}

