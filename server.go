package etly

import (
	"fmt"
	"github.com/viant/toolbox"
	"log"
	"net/http"
)

const uriBasePath = "/etly/"

type Server struct {
	config  *ServerConfig
	Service *Service
}

func (s *Server) Start() (err error) {
	err = s.Service.Start()
	if err != nil {
		return err
	}
	defer s.Service.Stop()
	logger.Printf("Starting ETL service on port %v", s.config.Port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%v", s.config.Port), nil))
	return nil
}

func NewServer(config *ServerConfig, transferConfig *TransferConfig) (*Server, error) {
	service, err := NewService(config, transferConfig)
	if err != nil {
		return nil, err
	}
	var result = &Server{
		config:  config,
		Service: service,
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
		toolbox.ServiceRouting{
			HTTPMethod: "GET",
			URI:        uriBasePath + "errors",
			Handler:    service.GetErrors,
			Parameters: []string{},
		},
		toolbox.ServiceRouting{
			HTTPMethod: "GET",
			URI:        uriBasePath + "info",
			Handler:    service.ProcessingStatus,
			Parameters: []string{"name"},
		},
		toolbox.ServiceRouting{
			HTTPMethod: "POST",
			URI:        uriBasePath + "transfer",
			Handler:    service.transferObjectService.Transfer,
			Parameters: []string{"request"},
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
