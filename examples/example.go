package main

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"gitlab.paradise-soft.com.tw/backend/yaitoo/tracer"

	"gitlab.paradise-soft.com.tw/dwh/dispatcher/service"

	"gitlab.paradise-soft.com.tw/dwh/dispatcher/glob"

	"gitlab.paradise-soft.com.tw/dwh/dispatcher"
)

var (
	received int
	errCount int
)

func main() {
	// dispatcher.Subscribe(topic, callback, 5)
	testCount := 5
	Integration(testCount)
	fmt.Printf("Produced: %v, Received: %v, Err: %v", testCount, received, errCount)
	// fmt.Println(service.TopicService.List())

	time.Sleep(time.Hour)
}

func Integration(msgCount int) (received, errCount int) {
	received = 0
	errCount = 0

	ConsumerGroup(glob.Config.Topic)
	// Consumer(glob.Config.Topic)
	time.Sleep(5 * time.Second)

	Producer(glob.Config.Topic, msgCount)

	sleepTime := 15
	if msgCount >= 10000 {
		sleepTime = 40
	}
	time.Sleep(time.Duration(sleepTime) * time.Second)

	return
}

func MultiConsProds(msgCount int) int {
	received = 0
	errCount = 0

	service.TopicService.Remove("disp.test.1")
	service.TopicService.Remove("disp.test.2")
	time.Sleep(1 * time.Second)

	ConsumerGroup("disp.test.1")
	ConsumerGroup("disp.test.2")
	time.Sleep(8 * time.Second)

	Producer("disp.test.1", msgCount)
	Producer("disp.test.2", msgCount)
	time.Sleep(10 * time.Second)
	return received
}

func ConsumerGroup(topic string) {
	dispatcher.SubscribeGroup(topic, "my-group-id", callback, 5)
}

func Consumer(topic string) {
	dispatcher.Subscribe(topic, callback, 5)
}

func Producer(topic string, count int) {
	for i := 1; i <= count; i++ {
		msg := fmt.Sprintf("%v.-%v (%v)", i, time.Now().Format("01/02 15:04:05"), topic)
		dispatcher.Send(topic, []byte("go-dis-"+strconv.Itoa(i)), []byte(msg), errorHandler)
	}
}

func errorHandler(key, value []byte, err error) {
	tracer.Errorf("TEST", " Producer收到consumer回傳的error: %v/%v/%v", string(key[:]), glob.TrimBytes(value), err.Error())
	errCount++
}

func callback(key, value []byte) error {
	received++
	return nil
}

func callbackERR(key, value []byte) error {
	received++
	return errors.New("測試錯誤唷~")
}
