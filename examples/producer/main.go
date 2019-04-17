package main

import (
	"strconv"
	"time"

	"gitlab.paradise-soft.com.tw/dwh/dispatcher"
	"gitlab.paradise-soft.com.tw/dwh/dispatcher/glob"
)

func main() {
	for i := 1; i <= 50; i++ {
		dispatcher.Send(glob.Config.Topic, []byte("go-dis"), []byte(strconv.Itoa(i)+". ts-"+time.Now().Format("01/02 15:04:05")))
	}
	time.Sleep(500 * time.Millisecond) // Wait message sent complete
}
