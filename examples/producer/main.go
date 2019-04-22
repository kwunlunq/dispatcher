package main

import (
	"fmt"
	"strconv"
	"time"

	"gitlab.paradise-soft.com.tw/backend/yaitoo/tracer"
	"gitlab.paradise-soft.com.tw/dwh/dispatcher"
	"gitlab.paradise-soft.com.tw/dwh/dispatcher/glob"
)

func main() {
	topic := glob.Config.Topic
	for i := 1; i <= 10; i++ {
		msg := fmt.Sprintf("%v.-%v (%v)", i, time.Now().Format("01/02 15:04:05"), topic)
		dispatcher.Send(topic, []byte("go-dis-"+strconv.Itoa(i)), []byte(msg), errorHandler)
	}
}

func errorHandler(key, value []byte, err error) {
	tracer.Errorf("TEST", " Producer收到consumer回傳的error: %v/%v/%v", string(key[:]), glob.TrimBytes(value), err.Error())
}
