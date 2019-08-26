package main

import (
	"errors"
	"fmt"
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

	// Basic usage
	errSignal := dispatcher.Subscribe(topic, callback)

	// With option(s)
	// errSignal := dispatcher.Subscribe(topic, callback, dispatcher.ConsumerSetAsyncNum(10))

	err, ok := <-errSignal
	if ok {
		fmt.Println(err)
	}

	// Listening ...
	time.Sleep(time.Hour)
}

func callback(value []byte) error {
	// Process message ...
	// return error if there's any
	return errors.New("來些測試錯誤")
}
