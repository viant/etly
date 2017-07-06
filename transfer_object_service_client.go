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
	index := int(atomic.AddUint64(&c.index, 1)) % len(c.cluster)
	return c.cluster[index]
}

func (c *transferObjectServiceClient) Transfer(request *TransferObjectRequest) *TransferObjectResponse {
	response := &TransferObjectResponse{}
	host := c.getNextHost()
	URL := fmt.Sprintf("http://%v:%v/etly/transfer", host.Server, host.Port)
	err := toolbox.RouteToServiceWithCustomFormat("post", URL, request, response,
		toolbox.NewJSONEncoderFactory(),
		toolbox.NewJSONDecoderFactory(),
		&toolbox.HttpOptions{Key: "TimeoutMs", Value: 240000})
	if err != nil {
		response.Error = fmt.Sprintf("failed to route to service:  %v, transfer(%v), err: %v", URL, request.SourceURL, err)
	}
	if response.Error != "" {
		response.Error = fmt.Sprintf("From host (%s), %s", URL, response.Error)
	}
	return response
}

func newTransferObjectServiceClient(cluster []*Host) TransferObjectService {
	return &transferObjectServiceClient{
		cluster: cluster,
	}
}
