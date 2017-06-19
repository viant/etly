package main

import (
	"github.com/viant/etly"
	"log"
	"flag"
)

/**
A sample server implementation for etly framework
*/
var (
	serverConfigURL = flag.String("serverConfigURL", "", "")
	transferConfigURL = flag.String("transferConfigURL", "", "")
)


func main() {
	flag.Parse()
	serverConfig, err := etly.NewServerConfigFromURL(*serverConfigURL)
	if err != nil {
		panic(err)
	}

	transferConfig, err := etly.NewTransferConfigFromURL(*transferConfigURL)
	if err != nil {
		panic(err)
	}
	service, err := etly.NewServer(serverConfig, transferConfig)
	if err != nil {
		panic(err)
	}
	log.Fatal(service.Start())
}
