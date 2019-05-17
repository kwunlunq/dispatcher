package main

import (
	"errors"
	"fmt"
	"time"

	"gitlab.paradise-soft.com.tw/glob/dispatcher/glob/core"
	"gitlab.paradise-soft.com.tw/glob/dispatcher/service"

	"gitlab.paradise-soft.com.tw/glob/dispatcher/glob"

	"gitlab.paradise-soft.com.tw/glob/dispatcher"
)

var (
	received int
	errCount int
	sent     int
	brokers  = []string{"10.200.252.180:9092", "10.200.252.181:9092", "10.200.252.182:9092"}
	groupID  = "kevin"
	topic    = "disp.testing"
)

func main() {
	testCount := 50

	start := time.Now()

	Integration(testCount)

	fmt.Printf("\n *** Summary ***\n * Test-Count: %v\n * Sent: %v\n * Received: %v\n * ErrCallback: %v\n * Cost: %vs\n\n", testCount, sent, received, errCount, int(time.Now().Sub(start).Seconds()))
	service.TopicService.Remove("disp.testing", "disp.testing_ERR")
	// time.Sleep(time.Hour)
}

func Integration(msgCount int) (int, int) {
	received = 0
	errCount = 0
	sent = 0

	dispatcher.Init(brokers, groupID, dispatcher.InitSetLogLevel("debug"))

	Producer(topic, msgCount)

	Consumer(topic)

	waitProducer(msgCount)
	waitConsumer(msgCount)
	return received, errCount
}

func Consumer(topic string) {
	dispatcher.Subscribe(topic, callbackERR, dispatcher.ConsumerSetAsyncNum(5))
	// waitComplete(func() bool { return received >= msgCount })
}

func Producer(topic string, msgCount int) {
	for i := 1; i <= msgCount; i++ {
		// msg := fmt.Sprintf("%v.-%v (%v)", i, time.Now().Format("01/02 15:04:05"), topic)
		msg := []byte(fmt.Sprintf("msg-val-%v", i))
		dispatcher.Send(topic, msg, dispatcher.ProducerAddErrHandler(errorHandler))
		sent++
	}

	// waitComplete(func() bool { return sent >= msgCount && errCount >= msgCount })
}

func errorHandler(value []byte, err error) {
	core.Logger.Debugf("Error from consumer: %v/%v", glob.TrimBytes(value), err.Error())
	errCount++
}

func callback(value []byte) error {
	received++
	// time.Sleep(1 * time.Second)
	return nil
}

func callbackERR(value []byte) error {
	received++
	// time.Sleep(1 * time.Second)
	return errors.New("測試錯誤唷~")
}

func waitProducer(msgCount int) {
	waitComplete(func() bool { return sent >= msgCount && errCount >= msgCount })
}

func waitConsumer(msgCount int) {
	waitComplete(func() bool { return received >= msgCount })
}

func waitComplete(condFn func() bool) {
	for {
		if condFn() {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func sleep(msgCount int) {
	sleepTime := 15
	if msgCount >= 10000 {
		sleepTime = 40
	}
	time.Sleep(time.Duration(sleepTime) * time.Second)

}

func MultiConsProds(msgCount int) int {
	received = 0
	errCount = 0

	service.TopicService.Remove("disp.test.1")
	service.TopicService.Remove("disp.test.2")
	time.Sleep(1 * time.Second)

	Consumer("disp.test.1")
	Consumer("disp.test.2")
	time.Sleep(8 * time.Second)

	Producer("disp.test.1", msgCount)
	Producer("disp.test.2", msgCount)
	time.Sleep(10 * time.Second)
	return received
}
