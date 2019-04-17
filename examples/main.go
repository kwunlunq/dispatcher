package main

import (
	"strconv"
	"time"

	"gitlab.paradise-soft.com.tw/backend/yaitoo/tracer"

	"gitlab.paradise-soft.com.tw/dwh/dispatcher"
	"gitlab.paradise-soft.com.tw/dwh/dispatcher/glob"
)

func main() {
	consumer()
	time.Sleep(5 * time.Second)
	producer()
	time.Sleep(30 * time.Second)
}

func consumer() {
	dispatcher.Subscribe(glob.Config.Topic, "my-group-id", nil, 5)
	// time.Sleep(time.Hour)
}

func producer() {
	for i := 1; i <= 50; i++ {
		dispatcher.Send(glob.Config.Topic, []byte("go-dis"), []byte(strconv.Itoa(i)+". ts-"+time.Now().Format("01/02 15:04:05")))
	}
	// time.Sleep(500 * time.Millisecond) // Wait message sent complete
}

func callback(key, value []byte) error {
	tracer.Tracef("TEST", "Received msg [%v/%v]\n", string(key[:]), string(value[:]))
	// time.Sleep(30 * time.Second)
	tracer.Tracef("TEST", "Processed msg [%v/%v]\n", string(key[:]), string(value[:]))
	return nil
}
