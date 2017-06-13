package etly

import (
	"sync"
)

var providerRegistry *ProviderRegistry
var providerRegistryMux = &sync.Mutex{}

type Provider func() interface{}

type ProviderRegistry struct {
	registry map[string]Provider
}

func (r *ProviderRegistry) Register(name string, provider Provider) {
	r.registry[name] = provider
}

func NewProviderRegistry() *ProviderRegistry {
	if providerRegistry != nil {
		return providerRegistry
	}
	providerRegistryMux.Lock()
	defer providerRegistryMux.Unlock()
	if providerRegistry != nil {
		return providerRegistry
	}
	providerRegistry = &ProviderRegistry{
		make(map[string]Provider),
	}
	return providerRegistry
}
