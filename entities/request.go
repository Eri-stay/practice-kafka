package entities

import "time"

type Request struct {
	Recipient    string     `json:"recipient"`
	Subject      string     `json:"subject"`
	Body         string     `json:"body"`
	ScheduleTime *time.Time `json:"schedule_time,omitempty"`
}