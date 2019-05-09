package main

import (
	"errors"
	"time"

	"gitlab.paradise-soft.com.tw/dwh/dispatcher"
)

var (
	brokers = []string{"10.200.252.180:9092", "10.200.252.181:9092", "10.200.252.182:9092"}
	groupID = "kevin"
	topic   = "disp.testing"
)

func main() {
	dispatcher.Init(brokers, groupID)

	// Basic usage
	dispatcher.Subscribe(topic, callback)

	// With option(s)
	// dispatcher.Subscribe(topic, callback, dispatcher.ConsumerSetAsyncNum(10))

	// Listening ...
	time.Sleep(time.Hour)
}

func callback(value []byte) error {
	// Process message ...
	// return error if there's one
	return errors.New("來些測試錯誤")
}
