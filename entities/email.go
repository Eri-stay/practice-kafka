package entities

type Email struct {
	Id        int    `json:"id"`
	Recipient string `json:"recipient"`
	Subject   string `json:"subject"`
	Body      string `json:"body"`
}
