package main

import (
	"time"

	"gitlab.paradise-soft.com.tw/dwh/dispatcher"
	"gitlab.paradise-soft.com.tw/dwh/dispatcher/glob"
)

func main() {
	dispatcher.Subscribe(glob.Config.Topic, callback, 5)
	time.Sleep(time.Hour) // Listening...
}

func callback(key, value []byte) error {
	// Process message ...
	// return error if there's one
	return nil
}
