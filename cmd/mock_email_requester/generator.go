package mock_email_requester

import (
	"math/rand"
	"time"

	"github.com/Eri-stay/practice-kafka/entities"
)

func generateRandomEmailRequest(isEmailValid bool) entities.Request {
	return entities.Request{
		Recipient: generateRandomEmailAdress(isEmailValid),
		Subject:   generateRandomSubject(),
		Body:      generateRandomBody(),
	}
}

func generateRandomEmailRequestWithSchedule(isEmailValid bool, minutes int) entities.Request {
	scheduleTime := time.Now().Add(time.Duration(minutes) * time.Minute)
	return entities.Request{
		Recipient:    generateRandomEmailAdress(isEmailValid),
		Subject:      generateRandomSubject(),
		Body:         generateRandomBody(),
		ScheduleTime: &scheduleTime,
	}
}

func generateRandomEmailAdress(isValid bool) string {
	if isValid {
		emails := []string{
			//"maria.chep.ua@gmail.com",
			"marichka.i.am@gmail.com",
		}
		return emails[rand.Intn(len(emails))]
	} else {
		letters := "aaaabcdeeeefghiiijklmnoooopqrstuuuvwxyz"
		email := ""
		for i := 0; i < 10; i++ {
			email += string(letters[rand.Intn(len(letters))])
		}
		return email + "example.com"
	}
}

func generateRandomSubject() string {
	subjects := []string{
		"~ Meeting Reminder",
		"~ Project Update",
		"~ Feedback Request",
		"~ Welcome Aboard",
		"~ Event Invitation",
	}
	return subjects[rand.Intn(len(subjects))] + " " + time.Now().Format("2006-01-02 15:04:05")
}

func generateRandomBody() string {
	body := `Lorem ipsum dolor sit amet, consectetur adipiscing elit. Fusce sit amet pretium urna, ut hendrerit ipsum. Proin viverra massa a hendrerit mattis. Aenean blandit eros libero.`
	return body[:rand.Intn(len(body))] + ".\nBest regards,\nMock Email Requester"
}
