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
	workerPort = flag.Int("workerPort", 8082, "worker -workerPort=8082")
)

func main() {
	flag.Parse()
	var config = &etly.ServerConfig{
		Port:*workerPort,
		Cluster:[]*etly.Host{},
	}
	transferConfig :=  &etly.TransferConfig{Transfers:[]*etly.Transfer{}}
	service, err := etly.NewServer(config,transferConfig)
	if err != nil {
		panic(err)
	}
	log.Fatal(service.Start())
}
