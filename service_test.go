package etly_test

import (
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/viant/etly"
	"github.com/viant/toolbox"
)

type Log1 struct {
	Id   int
	Name string
	Type string
}

type Log2 struct {
	Key   int
	Value string
}

var Log1ToLog2 = func(source interface{}) (interface{}, error) {
	log1, casted := source.(*Log1)
	if !casted {
		return nil, fmt.Errorf("Failed to cast source: %T, expected %T", source, &Log1{})
	}
	return &Log2{Key: log1.Id, Value: log1.Name + "/" + log1.Type}, nil
}

func init() {
	etly.NewTransformerRegistry().Register("service_test.Log1ToLog2", Log1ToLog2)
	etly.NewProviderRegistry().Register("service_test.Log1", func() interface{} {
		return &Log1{}
	})
}
