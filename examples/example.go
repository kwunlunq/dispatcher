package main

import (
	"errors"
	"fmt"
	"gitlab.paradise-soft.com.tw/glob/dispatcher"
	"sync/atomic"
	"time"
)

var (
	_testCount                            int
	_received, _errCount, _replied, _sent uint64
	_brokers                              = []string{"10.200.252.180:9092", "10.200.252.181:9092", "10.200.252.182:9092"}
	_groupID                              = ""
	_topic                                = "dispatcher.example.testing.kevin.2"
	_logLevel                             = "info"
	_showExampleLog                       = true
	//wgSend, wgReceive, wgErr, wgReply sync.WaitGroup
)

func main() {

	start := time.Now()

	Integration(500)

	time.Sleep(time.Second) // Wait for offsets to be marked
	fmt.Printf("\n *** Summary ***\n * Test-Count: %v\n * Sent: %v\n * Received: %v\n * Err received: %v\n * Reply received: %v\n * Cost: %vs\n\n", _testCount, _sent, _received, _errCount, _replied, int(time.Now().Sub(start).Seconds()))
}

// Integration 整合測試: 傳送 + 接收
func Integration(msgCount int) (int, int, int) {
	_testCount = msgCount
	_sent, _received, _errCount, _replied = 0, 0, 0, 0

	_ = dispatcher.Init(_brokers, dispatcher.InitSetLogLevel(_logLevel), dispatcher.InitSetDefaultGroupID(_groupID))

	go send(_topic, msgCount)

	go consumeWithRetry(_topic)

	waitComplete()
	return int(_received), int(_errCount), int(_replied)
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
		//_ = dispatcher.Send(topic, msg, dispatcher.ProducerAddErrHandler(errorHandler), dispatcher.ProducerCollectReplyMessage(replyHandler, time.Minute))
		_ = dispatcher.Send(topic, msg, dispatcher.ProducerAddErrHandler(errorHandler), dispatcher.ProducerCollectReplyMessage(replyHandler, dispatcher.NoTimeout))
		//_ = dispatcher.Send(_topic, msg)
		atomic.AddUint64(&_sent, 1)
	}
}

func msgHandler(value []byte) error {
	atomic.AddUint64(&_received, 1)
	if _showExampleLog {
		fmt.Printf("MSG | %v/%v | %v \n", atomic.LoadUint64(&_received), _testCount, string(value))
	}
	return errors.New("錯誤: 測試錯誤, 訊息: " + string(value))
}

func errorHandler(value []byte, err error) {
	atomic.AddUint64(&_errCount, 1)
	if err == nil {
		err = errors.New("")
	}
	if _showExampleLog {
		fmt.Printf("ERR | %v/%v | %v | %v\n", atomic.LoadUint64(&_errCount), _testCount, err.Error(), string(value))
	}
}

func replyHandler(message dispatcher.Message, err error) {
	atomic.AddUint64(&_replied, 1)
	if err == nil {
		fmt.Println("Err receiving reply: ", err)
	}
	if _showExampleLog {
		fmt.Printf("Rep | %v/%v | %v | %v | %v | %v | %v\n", atomic.LoadUint64(&_replied), _testCount, message.TaskID, message.ConsumerGroupID, message.ConsumerReceivedTime, string(message.Value), err)
	}
}

func waitComplete() {
	for {
		if _sent >= uint64(_testCount) && _errCount >= uint64(_testCount) && _replied >= uint64(_testCount) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
}
