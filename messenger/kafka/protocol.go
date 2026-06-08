package kafka

import "time"

type emailRequest struct {
	Recipient    string     `json:"recipient"`
	Subject      string     `json:"subject"`
	Body         string     `json:"body"`
	ScheduleTime *time.Time `json:"schedule_time,omitempty"`
}

type email struct {
	Id        int    `json:"id"`
	Recipient string `json:"recipient"`
	Subject   string `json:"subject"`
	Body      string `json:"body"`
}

type status string

const (
	StatusSuccess  status = "finished"
	StatusTempFail status = "failed"
	StatusPermFail status = "totally_failed"
)

type result struct {
	EmailId     int        `json:"email_id"`
	Status      status     `json:"status"`
	ErrorMsg    string     `json:"error_msg"`
	Executed_at *time.Time `json:"executed_at"`
}
