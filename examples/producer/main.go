package main

import (
	"time"

	"gitlab.paradise-soft.com.tw/dwh/dispatcher"
)

func main() {
	dispatcher.Send("my-topic", "msg-key", "msg-val-1")
	dispatcher.Send("my-topic", "msg-key", "msg-val-2")
	dispatcher.Send("my-topic", "msg-key", "msg-val-3")
	time.Sleep(500 * time.Millisecond) // Wait message sent complete
}
