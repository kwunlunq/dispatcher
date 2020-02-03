package main

import (
	"fmt"
	"gitlab.paradise-soft.com.tw/glob/dispatcher"
	"time"
)

var (
	_brokers     = []string{"10.200.252.180:9092", "10.200.252.181:9092", "10.200.252.182:9092"}
	_groupID     = "example.integration1:dispatcher.example.testing"
	_topic       = "dispatcher.example.testing"
	_monitorHost = "10.200.252.184:7777"
)

func main() {
	_ = dispatcher.Init(_brokers, dispatcher.InitSetMonitorHost(_monitorHost))
	for {
		consumeStatus, err := dispatcher.GetConsumeStatusByGroupID(_topic, _groupID)
		if err != nil {
			fmt.Println("Err: ", err)
			return
		}
		fmt.Printf("[Monitor] GroupID: %v / LagCount: %v\n", consumeStatus.GroupID, consumeStatus.LagCount)
		time.Sleep(5 * time.Second)
	}
}
