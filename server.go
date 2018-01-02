package etly

import (
	"fmt"
	"net/http"
	"net/http/pprof"

	"github.com/viant/toolbox"
)

const uriBasePath = "/etly/"

type Server struct {
	mux     *http.ServeMux
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
	logger.Fatal(http.ListenAndServe(fmt.Sprintf(":%v", s.config.Port), s.mux))
	return nil
}

func NewServer(config *ServerConfig, transferConfig *TransferConfig) (*Server, error) {
	service, err := NewService(config, transferConfig)
	if err != nil {
		return nil, err
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
			URI:        uriBasePath + "info/{name}",
			Handler:    service.ProcessingStatus,
			Parameters: []string{"name"},
		},
		toolbox.ServiceRouting{
			HTTPMethod: "POST",
			URI:        uriBasePath + "transfer",
			Handler:    service.transferObjectService.Transfer,
			Parameters: []string{"request"},
		},

		toolbox.ServiceRouting{
			HTTPMethod: "POST",
			URI:        uriBasePath + "transferOnce",
			Handler:    service.TransferOnce,
			Parameters: []string{"request"},
		},

		toolbox.ServiceRouting{
			HTTPMethod: "Get",
			URI:        uriBasePath + "version",
			Handler:    service.Version,
			Parameters: []string{},
		},
	)
	mux := http.NewServeMux()
	mux.HandleFunc(uriBasePath, func(writer http.ResponseWriter, reader *http.Request) {
		err := router.Route(writer, reader)
		if err != nil {
			logger.Printf("Route Error: %v", err)
			writer.WriteHeader(http.StatusInternalServerError)
		}
	})

	if !config.Production {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	}
	result := &Server{
		mux:     mux,
		config:  config,
		Service: service,
	}
	return result, nil
}
