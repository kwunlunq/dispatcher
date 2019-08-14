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
	dispatcher.Init(brokers)

	msg := []byte("msg-val")

	// Basic usage
	dispatcher.Send(topic, msg)

	// With option(s)
	// dispatcher.Send(topic, msg, dispatcher.ProducerAddErrHandler(errorHandler))

	// Sending ...
	time.Sleep(time.Second)
}

func errorHandler(value []byte, err error) {
	// Handle error from consumer ...
}
