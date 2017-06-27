package etly


type StatusInfoResponse struct {
	Error string
	Status []*ResourceStatusInfo
}


func NewStatusInfoResponse() *StatusInfoResponse {
	return &StatusInfoResponse{
		Status:make([]*ResourceStatusInfo, 0),
	}
}

type ResourceStatusInfo struct {
	Resource string
	Errors []*Error
	ResourceStatus map[string]*ProcessingStatus
	Status         *ProcessingStatus
}

func NewResourceStatusInfo() *ResourceStatusInfo {
	return &ResourceStatusInfo{
		ResourceStatus:make(map[string]*ProcessingStatus),

	}
}