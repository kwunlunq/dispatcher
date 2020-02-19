package main

import (
	"errors"
	"fmt"
	"gitlab.paradise-soft.com.tw/glob/dispatcher"
	"log"
	"net/http"
	_ "net/http/pprof"
	"sync/atomic"
	"time"
)

var (
	_brokers       = []string{"10.200.252.180:9092", "10.200.252.181:9092", "10.200.252.182:9092"}
	_groupID       = ""
	_topic         = "dispatcher.example"
	_topicCount    = 20
	_start         = time.Now()
	_logLevel      = "info"
	_received      = uint64(0)
	_expectedCount = 1000
)

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:0815", nil))
	}()
	//consume()
	consumeWithRetry()
	//consumeMultipleTopics()
	time.Sleep(time.Second) // wait for offset to be committed
}

func consume() {
	_ = dispatcher.Init(_brokers, dispatcher.InitSetKafkaConfig(dispatcher.KafkaConfig{TopicReplicationNum: 3, MinInsyncReplicas: 1}))
	ctrl, err := dispatcher.Subscribe(_topic, callback)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("start after ", time.Now().Sub(_start).Seconds(), "s")
	err = <-ctrl.Errors() // blocked
	if err != nil {
		fmt.Println(err)
	}
}

func consumeWithRetry() {
	_ = dispatcher.Init(_brokers, dispatcher.InitSetDefaultGroupID(_groupID), dispatcher.InitSetLogLevel(_logLevel))

	//Subscribe with retry
	ctrl := dispatcher.SubscribeWithRetry(_topic, callback, 5, func(failCount int) time.Duration { return 5 * time.Second }, dispatcher.ConsumerSetAsyncNum(1))

	wait(ctrl)
}

func consumeMultipleTopics() {
	for i := 1; i <= _topicCount; i++ {
		topic := fmt.Sprintf("%v.%v", _topic, i)
		go dispatcher.SubscribeWithRetry(topic, callback, 5, func(failCount int) time.Duration { return 5 * time.Second }, dispatcher.ConsumerSetAsyncNum(1))
	}
}

func callback(value []byte) error {
	atomic.AddUint64(&_received, 1)
	fmt.Printf("%v | %d | %v\n", time.Now().Format("15:04.000"), atomic.LoadUint64(&_received), string(value))
	return errors.New("只是一個測試錯誤") // error will be sent back to producer's errHandler
}

func wait(ctrl *dispatcher.SubscriberWithRetryCtrl) {
	waitChan := make(chan struct{})
	start := time.Now()
	last := 0

	go func() {
		for {
			if int(atomic.LoadUint64(&_received)) == _expectedCount {
				break
			}
			this := int(atomic.LoadUint64(&_received))
			if this > last {
				fmt.Printf("%v | %d\n", time.Now().Sub(start).Seconds(), atomic.LoadUint64(&_received))
				last = this
			}
			time.Sleep(500 * time.Millisecond)

		}
		waitChan <- struct{}{}
	}()

	go func() {
		err := <-ctrl.Errors() // block until error occurs
		if err != nil {
			fmt.Println(err.Error())
		}
		waitChan <- struct{}{}
	}()

	<-waitChan
	fmt.Printf("Finished consuming %v messages in %v s\n", _expectedCount, time.Now().Sub(start).Seconds())
}
