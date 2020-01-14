package main

import (
	"errors"
	"fmt"
	"gitlab.paradise-soft.com.tw/glob/dispatcher"
	"strconv"
	"sync/atomic"
	"time"
)

var (
	_testCount, _producerCount, _consumerCount int
	_received, _errCount, _replied, _sent      uint64
	_expectedSent, _expectedReceived           uint64
	_brokers                                   = []string{"10.200.252.180:9092", "10.200.252.181:9092", "10.200.252.182:9092"}
	_defaultGroupID                            = ""
	_groupIDPrefix                             = "dispatcher.group."
	_topic                                     = "dispatcher.example.testing.kevin"
	_logLevel                                  = "info"
	_showExampleLog                            = false
)

func main() {

	start := time.Now()

	Integration(100, 2, 1)

	printResult(start)
}

// Integration 整合測試多producer, consumer併發收送訊息場景
func Integration(testCount, producerCount, consumerCount int) (int, int, int, int) {

	initParams(testCount, producerCount, consumerCount)

	_ = dispatcher.Init(_brokers, dispatcher.InitSetLogLevel(_logLevel), dispatcher.InitSetDefaultGroupID(_defaultGroupID))

	// Producers
	for i := 1; i <= producerCount; i++ {
		go send(_topic, testCount)
	}

	// Consumers
	for i := 1; i <= consumerCount; i++ {
		go consumeWithRetry(_topic, _groupIDPrefix+strconv.Itoa(i))
	}

	waitComplete()
	time.Sleep(2 * time.Second) // Wait for offsets to be marked
	return int(_received), int(_errCount), int(_replied), int(_expectedReceived)
}

func ReplyTimeout() {
	_ = dispatcher.Init(_brokers, dispatcher.InitSetDefaultGroupID(_defaultGroupID))
}

func consumeWithRetry(topic string, groupID string) {
	failRetryLimit := 5
	getRetryDuration := func(failCount int) time.Duration { return time.Duration(failCount) * time.Second }

	err := dispatcher.SubscribeWithRetry(topic, msgHandler, failRetryLimit, getRetryDuration, dispatcher.ConsumerSetAsyncNum(100), dispatcher.ConsumerSetGroupID(groupID))

	if err != nil {
		fmt.Println(err.Error())
	}
}

func send(topic string, msgCount int) {
	for i := 1; i <= msgCount; i++ {
		msg := []byte(fmt.Sprintf("msg-val-%v-%v", i, time.Now().Format("15:04.999")))
		_ = dispatcher.Send(topic, msg, dispatcher.ProducerAddErrHandler(errorHandler), dispatcher.ProducerCollectReplyMessage(replyHandler, dispatcher.NoTimeout))
		atomic.AddUint64(&_sent, 1)
		if _showExampleLog {
			fmt.Printf("Sent | %v/%v | %v\n", _sent, _expectedSent, string(msg))
		}
	}
}

func msgHandler(value []byte) error {
	atomic.AddUint64(&_received, 1)
	if _showExampleLog {
		fmt.Printf("MSG | %v/%v | %v \n", atomic.LoadUint64(&_received), _expectedReceived, string(value))
	}
	return errors.New("錯誤: 測試錯誤, 訊息: " + string(value))
}

func errorHandler(value []byte, err error) {
	atomic.AddUint64(&_errCount, 1)
	if err == nil {
		err = errors.New("")
	}
	if _showExampleLog {
		fmt.Printf("ERR | %v/%v | %v | %v\n", atomic.LoadUint64(&_errCount), _expectedReceived, err.Error(), string(value))
	}
}

func replyHandler(message dispatcher.Message, err error) {
	atomic.AddUint64(&_replied, 1)
	if err != nil {
		fmt.Println("Err receiving reply: ", err)
	}
	if _showExampleLog {
		fmt.Printf("Rep | %v/%v | %v | %v | %v | %v | %v\n", atomic.LoadUint64(&_replied), _expectedReceived, message.TaskID, message.ConsumerGroupID, message.ConsumerReceivedTime, string(message.Value), err)
	}
}

func waitComplete() {
	for {
		if _sent >= _expectedSent && _errCount >= _expectedReceived && _replied >= _expectedReceived {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func initParams(testCount, producerCount, consumerCount int) {
	_testCount = testCount
	_producerCount = producerCount
	_consumerCount = consumerCount
	_sent, _received, _errCount, _replied = 0, 0, 0, 0
	_expectedSent = uint64(_testCount * _producerCount)
	_expectedReceived = _expectedSent * uint64(_consumerCount)
}

func printResult(start time.Time) {
	fmt.Printf("\n *** Summary ***\n"+
		" * Input\n"+
		" *   Test-Count: %v\n"+
		" *   Producer: %v\n"+
		" *   Consumer:%v\n"+
		" * Result\n"+
		" *   Sent: %v\n"+
		" *   Received: %v\n"+
		" *   Err received: %v\n"+
		" *   Reply received: %v\n"+
		" *   Cost: %vs\n\n",
		_testCount, _producerCount, _consumerCount, _sent, _received, _errCount, _replied, int(time.Now().Sub(start).Seconds()))
}
