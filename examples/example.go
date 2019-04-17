package main

import (
	"fmt"
	"time"

	"gitlab.paradise-soft.com.tw/dwh/dispatcher/glob"

	"gitlab.paradise-soft.com.tw/dwh/dispatcher"
)

var (
	// produced int = 50
	received int
)

func main() {
	testCount := 5
	received := Integration(testCount)
	fmt.Printf("Produced: %v, Received: %v", testCount, received)
}

func Integration(msgCount int) int {
	received = 0
	Consumer(glob.Config.Topic)
	time.Sleep(5 * time.Second)
	Producer(glob.Config.Topic, msgCount)
	time.Sleep(10 * time.Second)
	return received
}

func multipleConsProds(msgCount int) int {
	// service.TopicService.Remove("disp.test.1")
	// service.TopicService.Remove("disp.test.2")
	received = 0
	Consumer("disp.test.1")
	Consumer("disp.test.2")
	time.Sleep(5 * time.Second)
	Producer("disp.test.1", msgCount)
	Producer("disp.test.2", msgCount)
	time.Sleep(10 * time.Second)
	return received
}

func Consumer(topic string) {
	dispatcher.Subscribe(topic, "my-group-id", callback, 5)
}

func Producer(topic string, count int) {
	for i := 1; i <= count; i++ {
		msg := fmt.Sprintf("%v. ts-%v (%v)", i, time.Now().Format("01/02 15:04:05"), topic)
		dispatcher.Send(topic, []byte("go-dis"), []byte(msg))
	}
}

func callback(key, value []byte) error {
	// tracer.Tracef("TEST", "Received msg [%v/%v]\n", string(key[:]), string(value[:]))
	// tracer.Tracef("TEST", "Processed msg [%v/%v]\n", string(key[:]), string(value[:]))
	received++
	return nil
}
