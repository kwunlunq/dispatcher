package main

import (
	"errors"
	"fmt"
	"gitlab.paradise-soft.com.tw/glob/dispatcher"
	"net/http"
	_ "net/http/pprof"
	"time"
)

var (
	_brokers = []string{"10.200.252.180:9092", "10.200.252.181:9092", "10.200.252.182:9092"}
	_groupID  = ""
	_topic    = "dispatcher.example.testing"
	_start    = time.Now()
	_logLevel = "info"
)

func main() {
	//consume()
	//consumeInRealWorld()
	consumeWithRetry()
}

func consume() {
	_ = dispatcher.Init(_brokers, dispatcher.InitSetKafkaConfig(dispatcher.KafkaConfig{TopicReplicationNum: 5, MinInsyncReplicas: 1}))
	ctrl, _ := dispatcher.Subscribe(_topic, callback)
	fmt.Println("start after ", time.Now().Sub(_start).Seconds(), "s")
	<-ctrl.Errors() // blocked
}

func consumeInRealWorld() {
	// pprof
	go func() {
		_ = http.ListenAndServe("0.0.0.0:8000", nil)
	}()

	// Initialization
	_ = dispatcher.Init(_brokers, dispatcher.InitSetDefaultGroupID(_groupID))

	failCount := 0
	failRetryLimit := 5
	retryDuration := 3 * time.Second

	for {
		// Create subscriber
		subscriberCtrl, err := dispatcher.Subscribe(_topic, callback, dispatcher.ConsumerSetAsyncNum(150))

		// Handle subscriber creation error
		if err != nil {
			failCount++
			fmt.Println("Create consumer err:", err.Error(), ", counting:", failCount)
			if failCount >= failRetryLimit {
				fmt.Println("Error count reach limit, leaving now")
				break
			}
			time.Sleep(retryDuration)
			continue
		}

		// Subscriber created successfully, reset failCount
		failCount = 0

		// Shutdown subscriber manually
		go func() {
			time.Sleep(retryDuration)
			//fmt.Println("Stopping consumer")
			//subscriberCtrl.Stop()
		}()

		// Handle error during subscription
		consumeErr, _ := <-subscriberCtrl.Errors()
		if consumeErr != nil {
			failCount++
			fmt.Println("Consuming err:", consumeErr.Error(), ", counting:", failCount)
			if failCount >= failRetryLimit {
				fmt.Println("Error count reach limit, closing now")
				break
			}
			time.Sleep(retryDuration)
			continue
		}

		// Subscriber been stopped manually
		fmt.Println("Consumer terminated without error")
		time.Sleep(retryDuration)
	}
}

func consumeWithRetry() {
	// pprof
	go func() {
		_ = http.ListenAndServe("0.0.0.0:8000", nil)
	}()

	// Initialization
	_ = dispatcher.Init(_brokers, dispatcher.InitSetDefaultGroupID(_groupID), dispatcher.InitSetLogLevel(_logLevel))

	failRetryLimit := 5
	getRetryDuration := func(failCount int) time.Duration { return time.Duration(failCount) * time.Second }

	// Subscribe with retry
	err := dispatcher.SubscribeWithRetry(_topic, callback, failRetryLimit, getRetryDuration, dispatcher.ConsumerSetAsyncNum(100))

	if err != nil {
		fmt.Println(err.Error())
	}
}

func callback(value []byte) error {
	// Process message
	fmt.Println("receive message:", string(value))

	// return error if there's any, will be sent to producer's errHandler
	return errors.New("只是一個測試錯誤")
}
