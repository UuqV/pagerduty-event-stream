package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"net/http"
	"bytes"
)

const pagerDutyURL = "https://events.pagerduty.com/v2/enqueue"

type PagerDutyEvent struct {
	RoutingKey  string `json:"routing_key"`
	EventAction string `json:"event_action"`
	Payload     struct {
		Summary   string `json:"summary"`
		Source    string `json:"source"`
		Severity  string `json:"severity"`
	} `json:"payload"`
}

func marshalPagerDutyAlert(routingKey, summary string) error {
	event := PagerDutyEvent{
		RoutingKey:  routingKey,
		EventAction: "trigger",
	}
	event.Payload.Summary = summary
	event.Payload.Source = "kafka-alert-service"
	event.Payload.Severity = "critical"

	data, err := json.Marshal(event)
	if err != nil {
		return err
	}

	sendOrLogAlert()

	return nil
}

func sendOrLogAlert(data json) error {
	routingKey := os.Getenv("PAGERDUTY_ROUTING_KEY")
	if routingKey == "" {
		logAlert(data)
	}
	else {
		sendAlert(data)
	}

}

func logAlert(data json) error {
	fmt.Println(data)
}

func sendAlert(data json) error {
	resp, err := http.Post(pagerDutyURL, "application/json", bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return fmt.Errorf("pagerduty returned status %d", resp.StatusCode)
	}
}

func main() {
	// Kafka consumer config
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
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

			if err := sendPagerDutyAlert(routingKey, alert); err != nil {
				log.Printf("failed to send PagerDuty alert: %v", err)
			} else {
				log.Printf("Alert sent to PagerDuty: %s", alert)
			}
		}
	}
}
