package etly

import "time"

type ObjectMeta struct {
	Source              string
	Target              string
	RecordProcessed     int
	RecordSkipped       int
	Timestamp           time.Time
	ProcessingTimeInSec int
	Message             string `json:",omitempty"`
	Error               string `json:",omitempty"`
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

type Meta struct {
	URL                 string
	Processed           map[string]*ObjectMeta
	ProcessingTimeInSec int
	RecentTransfers     int
	Errors              []string
}

func (m *Meta) AddError(error string) {
	if len(m.Errors) == 0 {
		m.Errors = make([]string, 0)
	}
	m.Errors = append(m.Errors, error)
}

func NewMeta(URL string) *Meta {
	return &Meta{
		URL:       URL,
		Processed: make(map[string]*ObjectMeta),
	}
}
