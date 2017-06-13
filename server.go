package etly

import (
	"fmt"
	"github.com/viant/toolbox"
	"log"
	"net/http"
)

var uriBasePath = "/etly/"

type Server struct {
	config  *Config
	service *Service
}

func (s *Server) Start() (err error) {
	err = s.service.Start()
	if err != nil {
		return err
	}
	defer s.service.Stop()
	logger.Printf("Starting ETL service on port %v", s.config.Port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%v", s.config.Port), nil))
	return nil
}

func (s *Server) Stop() {
	s.service.Stop()
}

func NewServer(config *Config) (*Server, error) {
	service, err := NewService(config)
	if err != nil {
		return nil, err
	}
	var result = &Server{
		config:  config,
		service: service,
	}

	router := toolbox.NewServiceRouter(
		toolbox.ServiceRouting{
			HTTPMethod: "GET",
			URI:        uriBasePath + "tasks/{ids}",
			Handler:    service.GetTasks,
			Parameters: []string{"@httpRequest", "ids"},
		},
		toolbox.ServiceRouting{
			HTTPMethod: "GET",
			URI:        uriBasePath + "status",
			Handler:    service.Status,
			Parameters: []string{},
		},
	)
	http.HandleFunc(uriBasePath, func(writer http.ResponseWriter, reader *http.Request) {
		err := router.Route(writer, reader)
		if err != nil {
			writer.WriteHeader(http.StatusInternalServerError)
		}
	})
	return result, nil
}
