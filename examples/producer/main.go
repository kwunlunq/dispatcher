package main

import (
	"strconv"
	"time"

	"gitlab.paradise-soft.com.tw/dwh/dispatcher/glob"

	"gitlab.paradise-soft.com.tw/dwh/dispatcher"
)

func main() {
	for i := 0; i < 50; i++ {
		dispatcher.Send(glob.Topic, []byte("msg-key"), []byte("Hello, from go-dispatcher-"+strconv.Itoa(i)))
	}
	time.Sleep(500 * time.Millisecond) // Wait message sent complete
}
