package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	routingKey := os.Getenv("PAGERDUTY_ROUTING_KEY")
	if routingKey == "" {
		log.Fatal("PAGERDUTY_ROUTING_KEY env var is required")
	}

	// Kafka consumer config
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "kafka:9092",
		"group.id":          "pagerduty-alert-service",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatalf("failed to create consumer: %v", err)
	}
	defer c.Close()

	err = c.Subscribe("alerts", nil)
	if err != nil {
		log.Fatalf("failed to subscribe: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle Ctrl+C
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigchan
		fmt.Println("Shutting down...")
		cancel()
	}()

	fmt.Println("Listening for alerts on Kafka...")

	for {
		select {
		case <-ctx.Done():
			return
		default:
			msg, err := c.ReadMessage(-1)
			if err != nil {
				log.Printf("Consumer error: %v\n", err)
				continue
			}
			alert := string(msg.Value)
			fmt.Printf("Received alert: %s\n", alert)

			if err := SendPagerDutyAlert(routingKey, alert); err != nil {
				log.Printf("failed to send PagerDuty alert: %v", err)
			} else {
				log.Printf("Alert sent to PagerDuty: %s", alert)
			}
		}
	}
}
