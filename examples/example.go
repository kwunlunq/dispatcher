package main

import (
	"errors"
	"fmt"
	"gitlab.paradise-soft.com.tw/glob/dispatcher"
	"sync/atomic"
	"time"
)

var (
	testCount                         int
	received, errCount, replied, sent uint64
	brokers                           = []string{"10.200.252.180:9092", "10.200.252.181:9092", "10.200.252.182:9092"}
	groupID                           = ""
	topic                             = "dispatcher.example.testing.kevin.2"
	logLevel                          = "info"
	showExampleLog                    = true
	//wgSend, wgReceive, wgErr, wgReply sync.WaitGroup
)

func main() {

	start := time.Now()

	Integration(2000)

	time.Sleep(time.Second) // Wait for offsets to be marked
	fmt.Printf("\n *** Summary ***\n * Test-Count: %v\n * Sent: %v\n * Received: %v\n * Err received: %v\n * Reply received: %v\n * Cost: %vs\n\n", testCount, sent, received, errCount, replied, int(time.Now().Sub(start).Seconds()))
}

// Integration 整合測試: 傳送 + 接收
func Integration(msgCount int) (int, int, int) {
	testCount = msgCount
	sent, received, errCount, replied = 0, 0, 0, 0

	_ = dispatcher.Init(brokers, dispatcher.InitSetLogLevel(logLevel), dispatcher.InitSetDefaultGroupID(groupID))

	go send(topic, msgCount)

	go consumeWithRetry(topic)

	waitComplete()
	return int(received), int(errCount), int(replied)
}

// TODO: 測試consumer group rebalance場景

func consumeWithRetry(topic string) {
	failRetryLimit := 5
	getRetryDuration := func(failCount int) time.Duration { return time.Duration(failCount) * time.Second }

	err := dispatcher.SubscribeWithRetry(topic, msgHandler, failRetryLimit, getRetryDuration, dispatcher.ConsumerSetAsyncNum(100))

	if err != nil {
		fmt.Println(err.Error())
	}
}

func send(topic string, msgCount int) {
	for i := 1; i <= msgCount; i++ {
		msg := []byte(fmt.Sprintf("msg-val-%v-%v", i, time.Now().Format("15:04.999")))
		_ = dispatcher.Send(topic, msg, dispatcher.ProducerAddErrHandler(errorHandler), dispatcher.ProducerCollectReplyMessage(replyHandler, time.Minute))
		//_ = dispatcher.Send(topic, msg)
		atomic.AddUint64(&sent, 1)
	}
}

func msgHandler(value []byte) error {
	atomic.AddUint64(&received, 1)
	if showExampleLog {
		fmt.Printf("MSG | %v/%v | %v \n", atomic.LoadUint64(&received), testCount, string(value))
	}
	return errors.New("錯誤: 測試錯誤, 訊息: " + string(value))
}

func errorHandler(value []byte, err error) {
	atomic.AddUint64(&errCount, 1)
	if err == nil {
		err = errors.New("")
	}
	if showExampleLog {
		fmt.Printf("ERR | %v/%v | %v | %v\n", atomic.LoadUint64(&errCount), testCount, err.Error(), string(value))
	}
}

func replyHandler(message dispatcher.Message, err error) {
	atomic.AddUint64(&replied, 1)
	if err == nil {
		err = errors.New("")
	}
	if showExampleLog {
		fmt.Printf("Rep | %v/%v | %v | %v | %v | %v | %v\n", atomic.LoadUint64(&replied), testCount, message.TaskID, message.ConsumerGroupID, message.ConsumerReceivedTime, string(message.Value), err.Error())
	}
}

func waitComplete() {
	for {
		if sent >= uint64(testCount) && errCount >= uint64(testCount) && replied >= uint64(testCount) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
}
