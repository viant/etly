package etly

import (
	"fmt"
	"sync/atomic"

	"github.com/viant/toolbox"
	"time"
)

func init() {
}

type transferObjectServiceClient struct {
	cluster       []*Host
	index         uint64
	toolboxClient *toolbox.ToolboxHTTPClient
}

func (c *transferObjectServiceClient) getNextHost() *Host {
	index := int(atomic.AddUint64(&c.index, 1)) % len(c.cluster)
	return c.cluster[index]
}

func (c *transferObjectServiceClient) Transfer(request *TransferObjectRequest) *TransferObjectResponse {
	response := &TransferObjectResponse{}
	host := c.getNextHost()
	URL := fmt.Sprintf("http://%v:%v/etly/transfer", host.Server, host.Port)
	err := c.toolboxClient.Request("post", URL, request, response,
		toolbox.NewJSONEncoderFactory(),
		toolbox.NewJSONDecoderFactory())
	if err != nil {
		response.Error = fmt.Sprintf("failed to route to service:  %v, transfer(%v), err: %v", URL, request.SourceURL, err)
	}
	if response.Error != "" {
		response.Error = fmt.Sprintf("From host (%s), %s", URL, response.Error)
	}
	return response
}

func newTransferObjectServiceClient(cluster []*Host, timeOut *Duration) TransferObjectService {
	//Current API code was not designed to return error so panic here incase there is any misconfiguration
	if timeOut == nil {
		panic("Timeout is not configured")
	}

	d, err := timeOut.Get()
	if err != nil {
		panic(err)
	}

	timeOutMSec := int(d / time.Millisecond) 	//Convert to msec
	toolboxClient, err := toolbox.NewToolboxHTTPClient(&toolbox.HttpOptions{Key: "TimeoutMs", Value: timeOutMSec})
	if err != nil {
		panic(err)
	}
	return &transferObjectServiceClient{
		cluster:       cluster,
		toolboxClient: toolboxClient,
	}
}
