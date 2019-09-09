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
	topic    = "dispatcher.example.testing"
)

func main() {
	testCount := 50

	start := time.Now()

	Integration(testCount)

	time.Sleep(time.Second) // Wait for offsets to be marked
	fmt.Printf("\n *** Summary ***\n * Test-Count: %v\n * Sent: %v\n * Received: %v\n * ErrCallback: %v\n * Cost: %vs\n\n", testCount, sent, received, errCount, int(time.Now().Sub(start).Seconds()))
	//removeUsedTopic()
}

// Integration 整合測試: 傳送 + 接收
func Integration(msgCount int) (int, int) {
	received = 0
	errCount = 0
	sent = 0

	_ = dispatcher.Init(brokers, dispatcher.InitSetLogLevel("debug"), dispatcher.InitSetDefaultGroupID(groupID))

	send(topic, msgCount)

	//go consume(topic)
	go consumeWithRetry(topic)

	waitProducer(msgCount)
	waitConsumer(msgCount)
	return received, errCount
}

func consume(topic string) {
	subCtrl, err := dispatcher.Subscribe(topic, callbackERR, dispatcher.ConsumerSetAsyncNum(5))
	fmt.Println("Subscribe started")
	if err != nil {
		fmt.Println("Subscribe error: ", err.Error())
		return
	}
	consumeErr, _ := <-subCtrl.Errors()
	if consumeErr != nil {
		fmt.Println("Subscribe error: ", consumeErr.Error())
	}
}

func consumeWithRetry(topic string) {
	failRetryLimit := 5
	getRetryDuration := func(failCount int) time.Duration { return time.Duration(failCount) * time.Second }

	// Subscribe with retry
	err := dispatcher.SubscribeWithRetry(topic, callbackERR, failRetryLimit, getRetryDuration, dispatcher.ConsumerSetAsyncNum(100))

	if err != nil {
		fmt.Println(err.Error())
	}
}

func send(topic string, msgCount int) {
	for i := 1; i <= msgCount; i++ {
		msg := []byte(fmt.Sprintf("msg-val-%v", i))
		_ = dispatcher.Send(topic, msg, dispatcher.ProducerAddErrHandler(errorHandler))
		sent++
	}
}

func errorHandler(value []byte, err error) {
	core.Logger.Debugf("Error from consumer: %v/%v", glob.TrimBytes(value), err.Error())
	errCount++
}

func callbackERR(value []byte) error {
	received++
	return errors.New("錯誤: 測試錯誤, 訊息: " + string(value))
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

func removeUsedTopic() {
	err := service.TopicService.Remove("disp.testing", "disp.testing_ERR", "disp.testing.kevin", "disp.testing.kevin_ERR", "disp.testing222", "disp.testing222_ERR")
	if err != nil {
		fmt.Println(err.Error())
	}
}

// (暫未使用) MultiPubSubs 測試: 多個 pub/sub 場景
func MultiPubSubs(msgCount int) int {
	received = 0
	errCount = 0

	_ = service.TopicService.Remove("disp.test.1")
	_ = service.TopicService.Remove("disp.test.2")
	time.Sleep(1 * time.Second)

	consume("disp.test.1")
	consume("disp.test.2")
	time.Sleep(8 * time.Second)

	send("disp.test.1", msgCount)
	send("disp.test.2", msgCount)
	time.Sleep(10 * time.Second)
	return received
}
