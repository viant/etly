package etly

import (
	"sync"
)

var providerRegistry *MessageProviderRegistry

type MessageProvider func() interface{}

type MessageProviderRegistry struct {
	lock     sync.Mutex
	registry map[string]MessageProvider
}

func (r *MessageProviderRegistry) Register(name string, provider MessageProvider) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.registry[name] = provider
}

func NewProviderRegistry() *MessageProviderRegistry {
	if providerRegistry != nil {
		return providerRegistry
	}
	providerRegistry = &MessageProviderRegistry{
		registry: make(map[string]MessageProvider),
	}
	return providerRegistry
}
