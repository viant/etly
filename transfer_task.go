package etly

import (
	"fmt"
	"math/rand"
	"time"
)

//TransferTask represent a transfer ETL task
type TransferTask struct {
	*Task
	Progress *TransferProgress
	Transfer *Transfer
}

//NewTransferTask create a new transfer task
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
		Id:       fmt.Sprintf("%v-%v-%v", transfer.Name, time.Now().UnixNano(), rand.NewSource(time.Now().UnixNano()).Int63()),
		Progress: result.Progress,
	}
	result.Task = task
	return result
}
