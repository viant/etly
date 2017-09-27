package etly

import "time"
import "sync"

type ObjectMeta struct {
	Source          string
	Target          string
	RecordProcessed int
	RecordSkipped   int
	Timestamp       time.Time
	Message         string
	Error           string
}

func NewObjectMeta(source, target, message, err string, recordProcessed, recordSkipped int, starTime *time.Time) *ObjectMeta {
	return &ObjectMeta{
		Source:          source,
		Target:          target,
		Message:         message,
		RecordProcessed: recordProcessed,
		RecordSkipped:   recordSkipped,
		Timestamp:       time.Now(),
		Error:           err,
	}
}

type ProcessingStatus struct {
	ResourceProcessed int
	ResourcePending   int
	RecordProcessed   int
}

type ResourcedMeta struct {
	Meta     *Meta
	Resource *Resource
}

type Meta struct {
	URL            string
	Processed      map[string]*ObjectMeta
	Errors         []*Error
	ResourceStatus map[string]*ProcessingStatus
	Status         *ProcessingStatus
	lock           sync.Mutex
}

func (m *Meta) PutStatus(source string, status *ProcessingStatus) {
	if len(m.ResourceStatus) == 0 {
		m.ResourceStatus = make(map[string]*ProcessingStatus)
	}
	m.ResourceStatus[source] = status

	total := &ProcessingStatus{}
	for _, sourceStatus := range m.ResourceStatus {
		total.ResourceProcessed += sourceStatus.ResourceProcessed
		total.ResourcePending += sourceStatus.ResourcePending
		total.RecordProcessed += sourceStatus.RecordProcessed
	}
	m.Status = total
}

type Error struct {
	Error string
	Time  time.Time
}

func (m *Meta) AddError(err string) {
	if len(m.Errors) == 0 {
		m.Errors = make([]*Error, 0)
	}
	m.Errors = append(m.Errors, &Error{
		Error: err,
		Time:  time.Now(),
	})
}

func NewMeta(URL string) *Meta {
	return &Meta{
		URL:       URL,
		Processed: make(map[string]*ObjectMeta),
	}
}
