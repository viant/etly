package etly

import "time"

type ObjectMeta struct {
	Source              string
	Target              string
	RecordProcessed     int
	RecordSkipped       int
	Timestamp           time.Time
	ProcessingTimeInSec int
}

type Meta struct {
	URL                 string
	Processed           map[string]*ObjectMeta
	ProcessingTimeInSec int
	RecentTransfers     int
}

func NewMeta(URL string) *Meta {
	return &Meta{
		URL:       URL,
		Processed: make(map[string]*ObjectMeta),
	}
}
