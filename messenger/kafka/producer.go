package kafka

import (
	"encoding/json"
	"fmt"

	"github.com/Eri-stay/practice-kafka/config"
	"github.com/Eri-stay/practice-kafka/entities"
	"github.com/Eri-stay/practice-kafka/messenger"
	"github.com/IBM/sarama"
)

var _ messenger.Producer = (*saramaProducer)(nil)

type saramaProducer struct {
	syncProducer sarama.SyncProducer
	cfg          *config.Config
}

func NewProducer(appConfig *config.Config) (*saramaProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	producer, err := sarama.NewSyncProducer(appConfig.KafkaBrokers, config)
	if err != nil {
		return nil, fmt.Errorf("start Sarama producer: %w", err)
	}
	return &saramaProducer{
		syncProducer: producer,
		cfg:          appConfig,
	}, nil
}

func (p *saramaProducer) produce(topic string, message []byte) error {
	msg := &sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(message)}

	_, _, err := p.syncProducer.SendMessage(msg)
	return err
}

func (p *saramaProducer) Close() error {
	return p.syncProducer.Close()
}

func (p *saramaProducer) EmailRequestEvent(event entities.Request) error {
	request := emailRequest{
		Recipient:    event.Recipient,
		Subject:      event.Subject,
		Body:         event.Body,
		ScheduleTime: event.ScheduleTime,
	}
	bytes, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("marshal email request: %w", err)
	}

	if err := p.produce(p.cfg.TopicEmailRequests, bytes); err != nil {
		return fmt.Errorf("produce email request: %w", err)
	}
	return nil
}

func (p *saramaProducer) ExecutionRequestEvent(event entities.Email) error {
	email := email{
		Id:        event.Id,
		Recipient: event.Recipient,
		Subject:   event.Subject,
		Body:      event.Body,
	}
	bytes, err := json.Marshal(email)
	if err != nil {
		return fmt.Errorf("marshal execution request: %w", err)
	}

	if err := p.produce(p.cfg.TopicEmailExecute, bytes); err != nil {
		return fmt.Errorf("produce execution request: %w", err)
	}
	return nil
}

func (p *saramaProducer) ExecutionResultEvent(event entities.Result) error {
	result := result{
		EmailId:     event.EmailId,
		Status:      status(event.Status),
		ErrorMsg:    event.ErrorMsg,
		Executed_at: event.Executed_at,
	}
	bytes, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("marshal execution result: %w", err)
	}

	if err := p.produce(p.cfg.TopicEmailResults, bytes); err != nil {
		return fmt.Errorf("produce execution result: %w", err)
	}
	return nil
}
