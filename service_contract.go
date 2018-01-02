package etly

import "time"

type DoRequest struct {
	Transfers []*Transfer
}

type DoResponse struct {
	Status string
	Error string
	StartTime time.Time
	EndTime time.Time
	Tasks []*TransferTask
}
