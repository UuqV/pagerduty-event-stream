package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"net/http"
)

const pagerDutyURL = "https://events.pagerduty.com/v2/enqueue"

// PagerDutyEvent models the request payload for Events API v2
type PagerDutyEvent struct {
	RoutingKey  string `json:"routing_key"`
	EventAction string `json:"event_action"`
	Payload     struct {
		Summary  string `json:"summary"`
		Source   string `json:"source"`
		Severity string `json:"severity"`
	} `json:"payload"`
}

// SendPagerDutyAlert sends a trigger event to PagerDuty
func SendPagerDutyAlert(summary string) error {

	alert, err := PreparePagerDutyAlert(summary)
	if err != nil {
		return err
	}
	DispatchPagerDutyAlert(alert)

	return nil
}

func PreparePagerDutyAlert(summary string) ([]byte, error) {
	routingKey := os.Getenv("PAGERDUTY_ROUTING_KEY")
	if routingKey == "" {
		return nil, fmt.Errorf("Pagerduty is not set up")
	}

	event := PagerDutyEvent{
		RoutingKey:  routingKey,
		EventAction: "trigger",
	}
	event.Payload.Summary = summary
	event.Payload.Source = "kafka-alert-service"
	event.Payload.Severity = "critical"

	data, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func DispatchPagerDutyAlert(data []byte) error {
	resp, err := http.Post(pagerDutyURL, "application/json", bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return fmt.Errorf("pagerduty returned status %d", resp.StatusCode)
	}
	return nil
}
