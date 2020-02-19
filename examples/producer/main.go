package main

import (
	"fmt"
	"gitlab.paradise-soft.com.tw/glob/dispatcher"
	"sync"
	"time"
)

var (
	_brokers      = []string{"10.200.252.180:9092", "10.200.252.181:9092", "10.200.252.182:9092"}
	_topic        = "dispatcher.example"
	_groupID      = "example.producer"
	_messageCount = 5000
	_logLevel     = "info"
)

func main() {
	_ = dispatcher.Init(_brokers, dispatcher.InitSetDefaultGroupID(_groupID), dispatcher.InitSetLogLevel(_logLevel))
	start := time.Now()
	var wg sync.WaitGroup
	for i := 1; i <= _messageCount; i++ {
		wg.Add(1)
		go send(i, &wg)
	}
	wg.Wait()
	fmt.Printf("%v message sent in %v s\n", _messageCount, time.Now().Sub(start).Seconds())
	time.Sleep(time.Second)
}

func send(i int, wg *sync.WaitGroup) {
	msg := []byte(fmt.Sprintf("message-%v-%v", i, time.Now().Format("15:04:05.999")))
	err := dispatcher.Send(_topic, msg)
	//err := dispatcher.Send(_topic, msg, dispatcher.ProducerAddErrHandler(errorHandler), dispatcher.ProducerCollectReplyMessage(replyHandler, 10*time.Second))
	if err != nil {
		fmt.Println("Error sending message: ", err)
		return
	}
	fmt.Println("Message sent:", string(msg))
	wg.Done()
}

// Handle error from consumer
func errorHandler(value []byte, err error) {
	fmt.Printf("Handle error from consumer: err: [%v], message: [%v]\n", err.Error(), string(value))
}

func replyHandler(message dispatcher.Message, err error) {
	now := time.Now().Format("15:04.000")
	if err != nil {
		fmt.Printf("Handle reply from consumer: err: %v, message: [%v], receivedAt: [%v]\n", err.Error(), string(message.Value), now)
	} else {
		fmt.Printf("Handle reply from consumer: message: [%v], receivedAt: [%v]\n", string(message.Value), now)
	}

}
