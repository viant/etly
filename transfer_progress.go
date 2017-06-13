package etly

import "time"

type TransferProgress struct {
	RecordProcessed int32
	RecordSkipped   int32
	FileProcessed   int32
	StartDate       string
	startTime       *time.Time
	ElapsedInSec    int32
}
