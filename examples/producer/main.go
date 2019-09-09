package main

import (
	"fmt"
	"gitlab.paradise-soft.com.tw/glob/dispatcher"
	"time"
)

var (
	brokers = []string{"10.200.252.180:9092", "10.200.252.181:9092", "10.200.252.182:9092"}
	topic   = "dispatcher.example.testing"
)

func main() {
	_ = dispatcher.Init(brokers)
	for i := 1; i <= 10; i++ {
		send(i)
		//sendInRealWorld(i)
	}
}

func send(i int) {
	msg := []byte(fmt.Sprintf("message-%v-%v", i, time.Now().Format("15:04:05.999")))
	_ = dispatcher.Send(topic, msg)
	fmt.Println("Message sent:", string(msg))
}

func sendInRealWorld(i int) {
	msg := []byte(fmt.Sprintf("message-%v-%v", i, time.Now().Format("15:04:05.999")))
	failCount := 0
	failRetryLimit := 5
	retryDuration := 3 * time.Second

	for {
		err := dispatcher.Send(topic, msg, dispatcher.ProducerAddErrHandler(errorHandler))

		// Handle send error
		if err != nil {
			failCount++
			if failCount >= failRetryLimit {
				fmt.Println("Error count reach limit")
				return
			}
			fmt.Println("Error sending message:", err.Error(), ", msg:", string(msg), ", count:", failCount)
			time.Sleep(retryDuration)
			continue
		}

		// Message sent
		fmt.Println("Message sent:", string(msg))
		break
	}
}

// Handle error from consumer
func errorHandler(value []byte, err error) {
	fmt.Println("Received error from subscriber's callback:", err.Error(), ", of message:", string(value))
}
