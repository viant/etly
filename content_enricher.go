package etly

import (
	"sync"
)

var contentEnricherRegistry *ContentEnricherRegistry

type ContentEnricher func(source interface{}, content interface{}) (interface{}, error)

type ContentEnricherRegistry struct {
	lock     sync.Mutex
	registry map[string]ContentEnricher
}

func (r *ContentEnricherRegistry) Register(name string, provider ContentEnricher) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.registry[name] = provider
}

func NewContentEnricherRegistry() *ContentEnricherRegistry {
	if contentEnricherRegistry != nil {
		return contentEnricherRegistry
	}
	contentEnricherRegistry = &ContentEnricherRegistry{
		registry: make(map[string]ContentEnricher),
	}
	return contentEnricherRegistry
}
