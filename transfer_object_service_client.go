package etly

import (
	"fmt"
	"sync/atomic"

	"github.com/viant/toolbox"
)

type transferObjectServiceClient struct {
	cluster []*Host
	index   uint64
}

func (c *transferObjectServiceClient) getNextHost() *Host {
	var index = int(atomic.AddUint64(&c.index, 1)) % len(c.cluster)
	return c.cluster[index]
}

func (s *transferObjectServiceClient) Transfer(request *TransferObjectRequest) *TransferObjectResponse {
	var response = &TransferObjectResponse{}
	var host = s.getNextHost()
	URL := string(fmt.Sprintf("http://%v:%v/etly/transfer", host.Server, host.Port))
	err := toolbox.RouteToService("post", URL, request, &response)
	if err != nil {
		response.Error = err.Error()
		response.ErrorReason = fmt.Sprintf("Failed to route to service:  %v", URL)
	}
	return response
}

func newTransferObjectServiceClient(cluster []*Host) TransferObjectService {
	return &transferObjectServiceClient{
		cluster: cluster,
	}
}
