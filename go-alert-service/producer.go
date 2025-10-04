package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Event structure with automatic timestamp
type Event struct {
	Type      string `json:"type"`
	Message   string `json:"message"`
	Timestamp int64  `json:"timestamp"` // RFC3339 formatted
}

// ProduceEvent creates an event with a timestamp and sends it to Kafka
func ProduceEvent(p *kafka.Producer, topic, eventType, message string) error {
	event := Event{
		Type:      eventType,
		Message:   message,
		Timestamp: time.Now().UTC().UnixMilli(),
	}

	value, err := json.Marshal(event)
	if err != nil {
		return err
	}

	// Produce asynchronously
	return p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          value,
	}, nil)
}

// RunProducer sets up the producer and sends events every 5 minutes
func RunProducer(brokers, topic string) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
	})
	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
	}
	defer p.Close()

	// Delivery report handler
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Printf("Delivery failed: %v", ev.TopicPartition.Error)
				}
			}
		}
	}()

	rand.Seed(time.Now().UnixNano())

	for {
		// Base interval 30ms
		base := 30 * time.Millisecond

		// Add jitter in [-10s, +10s]
		// With a 10s range, ~70% will be within ±5s of 30ms,
		// so ~30% will land outside and "fail"
		jitter := time.Duration(rand.Intn(21)-10) * time.Millisecond
		interval := base + jitter

		time.Sleep(interval)

		if err := ProduceEvent(p, topic, "heartbeat", "service is alive"); err != nil {
			log.Printf("Failed to send event: %v", err)
		}
	}

}
