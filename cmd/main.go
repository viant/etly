package main

import (
	"flag"
	"github.com/viant/etly"
	"log"
)

/**
A sample server implementation for etly framework
*/
var (
	configURL string
)

func init() {
	&configURL = flag.String("config url", "", "path or url to configuration file")
	if configURL == "" {
		log.Fatalln("Missing configuration url.")
	}
}

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
