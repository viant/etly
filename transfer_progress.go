package etly

import (
	"sync/atomic"
	"time"
)

type TransferProgress struct {
	RecordRead      int32
	RecordProcessed int32
	RecordSkipped   int32
	FileProcessed   int32
	BatchCount      int32
	StartDate       string
	startTime       *time.Time
	ElapsedInSec    int64
}

func (t *TransferProgress) Update(transfers ...*ProcessedTransfer) {
	for _, transfer := range transfers {
		atomic.AddInt32(&t.RecordSkipped, int32(transfer.RecordSkipped))
	}
}
