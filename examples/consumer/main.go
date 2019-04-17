package main

import (
	"time"

	"gitlab.paradise-soft.com.tw/backend/yaitoo/tracer"

	"gitlab.paradise-soft.com.tw/dwh/dispatcher"
	"gitlab.paradise-soft.com.tw/dwh/dispatcher/glob"
)

func main() {
	dispatcher.Subscribe(glob.Config.Topic, "my-group-id", callback, 5)
	time.Sleep(time.Hour)
}

func callback(key, value []byte) error {
	tracer.Tracef("TEST", "Received msg [%v/%v]\n", string(key[:]), string(value[:]))
	time.Sleep(30 * time.Second)
	tracer.Tracef("TEST", "Processed msg [%v/%v]\n", string(key[:]), string(value[:]))
	return nil
}
