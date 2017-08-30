package etly

import (
	"fmt"
	"sync/atomic"
	"time"

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

func (c *transferObjectServiceClient) Transfer(request *TransferObjectRequest) *TransferObjectResponse {
	response := &TransferObjectResponse{}
	host := c.getNextHost()
	URL := fmt.Sprintf("http://%v:%v/etly/transfer", host.Server, host.Port)
	err := toolbox.RouteToServiceWithCustomFormat("post", URL, request, response,
		toolbox.NewJSONEncoderFactory(),
		toolbox.NewJSONDecoderFactory(),
		&toolbox.HttpOptions{Key: "TimeoutMs", Value: time.Minute * 10})
	if err != nil {
		response.Error = fmt.Sprintf("Failed to route to service:  %v", URL)
	}
	return response
}

func newTransferObjectServiceClient(cluster []*Host) TransferObjectService {
	return &transferObjectServiceClient{
		cluster: cluster,
	}
}
