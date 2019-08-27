package main

import (
	"errors"
	"fmt"
	"gitlab.paradise-soft.com.tw/glob/dispatcher"
)

var (
	brokers = []string{"10.200.252.180:9092", "10.200.252.181:9092", "10.200.252.182:9092"}
	groupID = "kevin"
	topic   = "disp.testing"
)

func main() {
	_ = dispatcher.Init(brokers)

	// Basic usage
	err := dispatcher.Subscribe(topic, callback) // blocked

	// With option(s)
	// errSignal := dispatcher.Subscribe(topic, callback, dispatcher.ConsumerSetAsyncNum(10))

	if err != nil {
		fmt.Println("Consumer err", err.Error())
	}

	// Listening ...
	//time.Sleep(time.Hour)
}

func callback(value []byte) error {
	// Process message ...
	fmt.Println("receive message:", string(value))
	// return error if there's any
	return errors.New("來些測試錯誤")
}
