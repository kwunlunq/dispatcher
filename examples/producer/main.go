package main

import (
	"time"

	"gitlab.paradise-soft.com.tw/glob/dispatcher"
)

var (
	brokers = []string{"10.200.252.180:9092", "10.200.252.181:9092", "10.200.252.182:9092"}
	groupID = "kevin"
	topic   = "disp.testing"
)

func main() {
	dispatcher.Init(brokers, groupID)

	msg := []byte("msg-val")

	// Basic usage
	dispatcher.Send(topic, msg)

	// With option(s)
	// dispatcher.Send(topic, msg, dispatcher.ProducerAddErrHandler(errorHandler))

	// Wait for message send complete
	time.Sleep(time.Hour)
}

func errorHandler(value []byte, err error) {
	// Handle error from consumer ...
}
