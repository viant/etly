package etly

import "time"

type ObjectMeta struct {
	Source              string
	Target              string
	RecordProcessed     int
	RecordSkipped       int
	Timestamp           time.Time
	ProcessingTimeInSec int
	Message             string
	Error               string
}

func NewObjectMeta(source, target, message, err string, recordProcessed, recordSkipped int, starTime *time.Time) *ObjectMeta {
	return &ObjectMeta{
		Source:              source,
		Target:              target,
		Message:             message,
		RecordProcessed:     recordProcessed,
		RecordSkipped:       recordSkipped,
		Timestamp:           time.Now(),
		ProcessingTimeInSec: int(time.Now().Unix() - starTime.Unix()),
		Error:               err,
	}
}



type ProcessingStatus struct {
	ResourceProcessed int
	ResourcePending   int
	RecordProcessed   int
}

type Meta struct {
	URL                 string
	Processed           map[string]*ObjectMeta
	ProcessingTimeInSec int
	RecentTransfers     int
	Errors              []*Error
	ResourceStatus      map[string]*ProcessingStatus
	Status              *ProcessingStatus
}

func (m *Meta) PutStatus(source string, status *ProcessingStatus) {
	if len(m.ResourceStatus) == 0 {
		m.ResourceStatus = make(map[string]*ProcessingStatus)
	}
	m.ResourceStatus[source] = status

	var total = &ProcessingStatus{}
	for _, sourceStatus := range m.ResourceStatus {
		total.ResourceProcessed += sourceStatus.ResourceProcessed
		total.ResourcePending += sourceStatus.ResourcePending
		total.RecordProcessed += sourceStatus.RecordProcessed
	}
	m.Status = total
}

type Error struct{
	Error string
	Time time.Time
}

func (m *Meta) AddError(error string) {
	if len(m.Errors) == 0 {
		m.Errors = make([]*Error, 0)
	}
	m.Errors = append(m.Errors, &Error{
		Error:error,
		Time:time.Now(),
	})
}

func NewMeta(URL string) *Meta {
	return &Meta{
		URL:       URL,
		Processed: make(map[string]*ObjectMeta),
	}
}
