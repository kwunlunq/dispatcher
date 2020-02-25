package main

import (
	"fmt"
	"gitlab.paradise-soft.com.tw/glob/dispatcher"
)

var (
	_brokers         = []string{"10.200.252.180:9092", "10.200.252.181:9092", "10.200.252.182:9092"}
	_topicsToRemoved = []string{"dispatcher.example", "dispatcher.example_ERR", "dispatcher.example_Reply"}
)

func main() {
	_ = dispatcher.Init(_brokers, dispatcher.InitSetLogLevel("info"))

	err := dispatcher.RemoveTopics(_topicsToRemoved...)
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}
	fmt.Println("Topics removed: ", _topicsToRemoved)
}
