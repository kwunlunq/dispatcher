package main

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"gitlab.paradise-soft.com.tw/glob/common/settings"

	"gitlab.paradise-soft.com.tw/backend/yaitoo/tracer"

	"gitlab.paradise-soft.com.tw/dwh/dispatcher/service"

	"gitlab.paradise-soft.com.tw/dwh/dispatcher/glob"

	"gitlab.paradise-soft.com.tw/dwh/dispatcher"
)

var (
	received int
	errCount int
	sent     int
)

func main() {
	testCount := settings.Config.GetValueAsInt(glob.ProjName, "test_count", 15)

	start := time.Now()

	Integration(testCount)

	fmt.Printf("\n *** Summary ***\n * Test-Count: %v\n * Sent: %v\n * Received: %v\n * ErrCallback: %v\n * Cost: %vs\n\n", testCount, sent, received, errCount, int(time.Now().Sub(start).Seconds()))
	time.Sleep(time.Hour)
}

func Integration(msgCount int) (int, int) {
	received = 0
	errCount = 0
	sent = 0

	Producer(glob.Config.Topic, msgCount)

	Consumer(glob.Config.Topic, msgCount)

	waitProducer(msgCount)
	waitConsumer(msgCount)
	return received, errCount
}

func ConsumerGroup(topic string) {
	dispatcher.SubscribeGroup(topic, "my-group-id", callbackERR, 5)
}

func Producer(topic string, msgCount int) {
	for i := 1; i <= msgCount; i++ {
		// msg := fmt.Sprintf("%v.-%v (%v)", i, time.Now().Format("01/02 15:04:05"), topic)
		dispatcher.Send(topic, []byte("go-dis-"+strconv.Itoa(i)), []byte("key"+strconv.Itoa(i)), errorHandler)
		sent++
	}

	// waitComplete(func() bool { return sent >= msgCount && errCount >= msgCount })
}

func Consumer(topic string, msgCount int) {
	dispatcher.Subscribe(topic, callbackERR, 5)
	// waitComplete(func() bool { return received >= msgCount })
}

func waitComplete(condFn func() bool) {
	for {
		if condFn() {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func errorHandler(key, value []byte, err error) {
	tracer.Tracef(glob.ProjName, " Producer收到consumer回傳的error: %v/%v/%v", string(key[:]), glob.TrimBytes(value), err.Error())
	errCount++
}

func callback(key, value []byte) error {
	received++
	// time.Sleep(1 * time.Second)
	return nil
}

func callbackERR(key, value []byte) error {
	received++
	// time.Sleep(1 * time.Second)
	return errors.New("測試錯誤唷~")
}

func waitProducer(msgCount int) {
	// waitComplete(func() bool { return sent >= msgCount && errCount >= msgCount })
	waitComplete(func() bool { return sent >= msgCount && errCount >= msgCount })
}

func waitConsumer(msgCount int) {
	waitComplete(func() bool { return received >= msgCount })
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

	ConsumerGroup("disp.test.1")
	ConsumerGroup("disp.test.2")
	time.Sleep(8 * time.Second)

	Producer("disp.test.1", msgCount)
	Producer("disp.test.2", msgCount)
	time.Sleep(10 * time.Second)
	return received
}
