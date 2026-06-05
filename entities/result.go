package entities

import "time"

type Status string

const (
	StatusSuccess  Status = "finished"
	StatusTempFail Status = "failed"
	StatusPermFail Status = "totally_failed"
)

type Result struct {
	EmailId    int
	Status     string
	ErrorMsg   string
	Created_at *time.Time
}
