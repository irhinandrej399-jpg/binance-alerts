// Пакет kafka публикует события через Sarama.
package kafka

import (
	"context"
	"example.com/binance-alerts/services/market-data/internal/prices"
	"fmt"
	"github.com/IBM/sarama"
	"sync"
	"time"
)

type sender interface {
	SendMessage(*sarama.ProducerMessage) (int32, int64, error)
	Close() error
}
type Publisher struct {
	mu       sync.Mutex
	topic    string
	factory  func() (sender, error)
	producer sender
	closed   bool
}

// Config ограничивает сетевые ожидания и требует подтверждения всех доступных ISR.
// На одном локальном брокере это подтверждение одной реплики, а не отказоустойчивый кластер.
func Config() *sarama.Config {
	c := sarama.NewConfig()
	c.Version = sarama.V3_6_0_0
	c.ClientID = "market-data"
	c.Producer.RequiredAcks = sarama.WaitForAll
	c.Producer.Return.Successes = true
	c.Producer.Retry.Max = 0
	c.Net.MaxOpenRequests = 1
	c.Producer.Partitioner = sarama.NewHashPartitioner
	c.Net.DialTimeout = 3 * time.Second
	c.Net.ReadTimeout = 5 * time.Second
	c.Net.WriteTimeout = 5 * time.Second
	c.Producer.Timeout = 3 * time.Second
	c.Metadata.Retry.Max = 0
	c.Metadata.Timeout = 5 * time.Second
	c.Metadata.AllowAutoTopicCreation = false
	return c
}

// New не подключается к Kafka, чтобы её недоступность не мешала приёму в outbox.
func New(brokers []string, topic string) (*Publisher, error) {
	if len(brokers) == 0 || topic == "" {
		return nil, fmt.Errorf("brokers and topic are required")
	}
	conf := Config()
	if err := conf.Validate(); err != nil {
		return nil, err
	}
	return &Publisher{topic: topic, factory: func() (sender, error) { return sarama.NewSyncProducer(brokers, conf) }}, nil
}
func (p *Publisher) Publish(ctx context.Context, r prices.Record) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.closed {
		return fmt.Errorf("publisher is closed")
	}
	if p.producer == nil {
		producer, err := p.factory()
		if err != nil {
			return fmt.Errorf("connect Kafka: %w", err)
		}
		p.producer = producer
	}
	_, _, err := p.producer.SendMessage(&sarama.ProducerMessage{Topic: p.topic, Key: sarama.ByteEncoder(r.Key), Value: sarama.ByteEncoder(r.Value), Headers: []sarama.RecordHeader{{Key: []byte("event_id"), Value: []byte(r.EventID)}}})
	if err != nil {
		return fmt.Errorf("publish Kafka: %w", err)
	}
	// Если отмена совпала с ack, outbox сохраняет запись pending; возможен безопасный повтор.
	return ctx.Err()
}
func (p *Publisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	if p.producer != nil {
		return p.producer.Close()
	}
	return nil
}
