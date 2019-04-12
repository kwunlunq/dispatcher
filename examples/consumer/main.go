package main

import (
	"fmt"
	"time"

	"gitlab.paradise-soft.com.tw/dwh/dispatcher"
)

func main() {
	dispatcher.Subscribe("my-topic", "my-group-id", func(key, value []byte) error {
		fmt.Printf("Processing msg: [key: %v, val: %v]\n", string(key[:]), string(value[:]))
		time.Sleep(2 * time.Second)
		return nil
	})
}
