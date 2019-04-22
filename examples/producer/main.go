package main

import (
	"time"

	"gitlab.paradise-soft.com.tw/dwh/dispatcher"
	"gitlab.paradise-soft.com.tw/dwh/dispatcher/glob"
)

func main() {
	dispatcher.Send(glob.Config.Topic, []byte("msg-key"), []byte("msg-val"), errorHandler)
	time.Sleep(time.Second) // Wait for message send complete
}

func errorHandler(key, value []byte, err error) {
	// Handle error from consumer ...
}
