package main

import (
	"encoding/json"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Event structure with automatic timestamp
type Event struct {
	Type      string `json:"type"`
	Message   string `json:"message"`
	Timestamp string `json:"timestamp"` // RFC3339 formatted
}

// ProduceEvent creates an event with a timestamp and sends it to Kafka
func ProduceEvent(p *kafka.Producer, topic, eventType, message string) error {
	event := Event{
		Type:      eventType,
		Message:   message,
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
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
				} else {
					log.Printf("Delivered to %v", ev.TopicPartition)
				}
			}
		}
	}()

	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		if err := ProduceEvent(p, topic, "heartbeat", "service is alive"); err != nil {
			log.Printf("Failed to send event: %v", err)
		}
		<-ticker.C
	}
}
