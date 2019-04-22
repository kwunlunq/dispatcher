package main

import (
	"time"

	"gitlab.paradise-soft.com.tw/dwh/dispatcher"
	"gitlab.paradise-soft.com.tw/dwh/dispatcher/glob"
)

func main() {
	dispatcher.SubscribeGroup(glob.Config.Topic, "my-group-id", callback, 5)
	time.Sleep(time.Hour)
}

func callback(key, value []byte) error {
	return nil
}
