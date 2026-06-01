package kafka

import (
	"fmt"

	"github.com/Eri-stay/practice-kafka/messenger"
	"github.com/IBM/sarama"
)

type saramaProducer struct {
	syncProducer sarama.SyncProducer
}

func NewProducer(brokers []string) (messenger.Producer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, fmt.Errorf("start Sarama producer: %w", err)
	}
	return &saramaProducer{
		syncProducer: producer,
	}, nil
}

func (p *saramaProducer) Produce(topic string, message []byte) error {
	msg := &sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(message)}

	_, _, err := p.syncProducer.SendMessage(msg)
	return err
}

func (p *saramaProducer) Close() error {
	return p.syncProducer.Close()
}
