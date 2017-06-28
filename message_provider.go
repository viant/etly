package etly

import (
	"sync"
)

var providerRegistry *MessageProviderRegistry
var providerRegistryMux = &sync.Mutex{}

type MessageProvider func() interface{}

type MessageProviderRegistry struct {
	registry map[string]MessageProvider
}

func (r *MessageProviderRegistry) Register(name string, provider MessageProvider) {
	r.registry[name] = provider
}

func NewProviderRegistry() *MessageProviderRegistry {
	if providerRegistry != nil {
		return providerRegistry
	}
	providerRegistryMux.Lock()
	defer providerRegistryMux.Unlock()
	if providerRegistry != nil {
		return providerRegistry
	}
	providerRegistry = &MessageProviderRegistry{
		make(map[string]MessageProvider),
	}
	return providerRegistry
}
