package main

import (
	"github.com/viant/etly"
	"log"
)

/**
A sample server implementation for etly framework
*/
var (
	configURL = "file://.../cmd/config.json"
)

func main() {
	conf, err := etly.NewConfigFromURL(configURL)
	if err != nil {
		panic(err)
	}
	service, err := etly.NewServer(conf)
	if err != nil {
		panic(err)
	}
	log.Fatal(service.Start())
}
