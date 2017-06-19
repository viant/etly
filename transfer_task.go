package etly

import (
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"
)

//TransferTask represent a Transfer ETL status
type TransferTask struct {
	*Task
	Progress *TransferProgress
	Transfer *Transfer
}

func (t *TransferTask) UpdateElapsed() {
	atomic.StoreInt32(&t.Progress.ElapsedInSec, int32(time.Now().Unix()-t.Progress.startTime.Unix()))
}

//NewTransferTask create a new Transfer status
func NewTransferTask(transfer *Transfer) *TransferTask {
	now := time.Now()
	result := &TransferTask{
		Transfer: transfer,
		Progress: &TransferProgress{
			startTime: &now,
			StartDate: now.String(),
		},
	}

	var task = &Task{
		Id:       fmt.Sprintf("%v-%v-%v", transfer.Name, now.UnixNano(), rand.NewSource(now.UnixNano()).Int63()),
		Progress: result.Progress,
	}
	result.Task = task
	return result
}

//NewTransferTaskForId create a new Transfer status
func NewTransferTaskForId(id string, transfer *Transfer) *TransferTask {
	var result = NewTransferTask(transfer)
	result.Id = id
	return result
}
