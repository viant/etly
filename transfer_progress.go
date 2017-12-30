package etly

import (
	"time"
	"sync/atomic"
)

type TransferProgress struct {
	RecordRead          int32
	RecordProcessed     int32
	RecordSkipped       int32
	FileProcessed       int32
	BatchCount          int32
	StartDate           string
	startTime           *time.Time
	ElapsedInSec        int64
}


func (t *TransferProgress) Update(transfers ...*ProcessedTransfer) {
	for _, transfer := range transfers {
		atomic.AddInt32(&t.RecordProcessed, int32(transfer.RecordProcessed))
		atomic.AddInt32(&t.RecordSkipped,  int32(transfer.RecordSkipped))
	}
}