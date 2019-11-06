package main

import (
	"errors"
	"fmt"
	"gitlab.paradise-soft.com.tw/glob/dispatcher"
	"time"
)

var (
	testCount      int
	received       int
	errCount       int
	replied        int
	sent           int
	brokers        = []string{"10.200.252.180:9092", "10.200.252.181:9092", "10.200.252.182:9092"}
	groupID        = ""
	topic          = "dispatcher.example.testing"
	logLevel       = "info"
	showExampleLog = false
)

func main() {

	start := time.Now()

	Integration(10)

	time.Sleep(time.Second) // Wait for offsets to be marked
	fmt.Printf("\n *** Summary ***\n * Test-Count: %v\n * Sent: %v\n * Received: %v\n * Err received: %v\n * Reply received: %v\n * Cost: %vs\n\n", testCount, sent, received, errCount, replied, int(time.Now().Sub(start).Seconds()))
	//removeUsedTopic()
}

// Integration 整合測試: 傳送 + 接收
func Integration(msgCount int) (int, int, int) {
	testCount = msgCount
	received = 0
	errCount = 0
	replied = 0
	sent = 0

	_ = dispatcher.Init(brokers, dispatcher.InitSetLogLevel(logLevel), dispatcher.InitSetDefaultGroupID(groupID))

	go send(topic, msgCount)

	//go consume(topic)
	go consumeWithRetry(topic)

	waitProducer(msgCount)
	waitConsumer(msgCount)
	return received, errCount, replied
}

// TODO: 測試consumer group rebalance場景

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
		msg := []byte(fmt.Sprintf("msg-val-%v-%v", i, time.Now().Format("15:04.999")))
		_ = dispatcher.Send(topic, msg, dispatcher.ProducerAddErrHandler(errorHandler), dispatcher.ProducerCollectReplyMessage(replyHandler, time.Minute))
		sent++
	}
}

func errorHandler(value []byte, err error) {
	errCount++
	if err == nil {
		err = errors.New("")
	}
	if showExampleLog {
		fmt.Printf("ERR | %v/%v | %v | %v\n", errCount, testCount, err.Error(), string(value))
	}
}

func replyHandler(message dispatcher.Message, err error) {
	replied++
	if err == nil {
		err = errors.New("")
	}
	if showExampleLog {
		fmt.Printf("Rep | %v/%v | %v | %v | %v\n", replied, testCount, message.ConsumerReceivedTime, string(message.Value), err.Error())
	}
}

func callbackERR(value []byte) error {
	received++
	if showExampleLog {
		fmt.Printf("MSG | %v/%v | %v \n", received, testCount, string(value))
	}
	return errors.New("錯誤: 測試錯誤, 訊息: " + string(value))
}

func waitProducer(msgCount int) {
	waitComplete(func() bool { return sent >= msgCount && errCount >= msgCount && replied >= msgCount })
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
	//err := service.TopicService.Remove("dispatcher.example.testing", "dispatcher.example.testing_ERR", "dispatcher.example.testing.kevin", "dispatcher.example.testing.kevin_ERR", "dispatcher.example.testing222", "dispatcher.example.testing222_ERR")
	//if err != nil {
	//	fmt.Println(err.Error())
	//}
}

// (暫未使用) MultiPubSubs 測試: 多個 pub/sub 場景
func MultiPubSubs(msgCount int) int {
	received = 0
	errCount = 0

	//_ = service.TopicService.Remove("dispatcher.example.testing.1")
	//_ = service.TopicService.Remove("dispatcher.example.testing.2")
	time.Sleep(1 * time.Second)

	consume("dispatcher.example.testing.1")
	consume("dispatcher.example.testing.2")
	time.Sleep(8 * time.Second)

	send("dispatcher.example.testing.1", msgCount)
	send("dispatcher.example.testing.2", msgCount)
	time.Sleep(10 * time.Second)
	return received
}
