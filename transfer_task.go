package etly

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

//TransferTask represent a Transfer ETL status
type TransferTask struct {
	*Task
	Progress *TransferProgress
	Transfer *Transfer
}

// UpdateElapsed updates the elapsed time
func (t *TransferTask) UpdateElapsed() {
	atomic.StoreInt64(&t.Progress.ElapsedInSec, int64(time.Since(*t.Progress.startTime).Seconds()))
}

//NewTransferTaskForID create a new Transfer status
func NewTransferTaskForID(id string, transfer *Transfer) *TransferTask {
	result := NewTransferTask(transfer)
	result.Id = id
	return result
}

//NewTransferTask create a new Transfer status
func NewTransferTask(transfer *Transfer) *TransferTask {
	now := time.Now()
	transfer.Init()
	result := &TransferTask{
		Transfer: transfer,
		Progress: &TransferProgress{
			startTime: &now,
			StartDate: now.String(),
		},
	}
	result.Task = &Task{
		Id:        fmt.Sprintf("%v-%v-%v", transfer.Name, now.UnixNano(), rand.NewSource(now.UnixNano()).Int63()),
		Progress:  result.Progress,
		StartTime: time.Now(),
		Mutex:     &sync.Mutex{},
	}
	result.Task.SubTransfers = make(map[string]*SubTransfer)
	return result
}
