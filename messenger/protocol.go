package messenger

import "time"

type EmailRequest struct {
	Recipient    string     `json:"recipient"`
	Subject      string     `json:"subject"`
	Body         string     `json:"body"`
	ScheduleTime *time.Time `json:"schedule_time,omitempty"`
}
