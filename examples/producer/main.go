package main

import (
	"gitlab.paradise-soft.com.tw/dwh/dispatcher"
)

func main() {
	dispatcher.Send("my-topic", "msg-key", "msg-val")
}
