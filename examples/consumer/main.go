package main

import (
	"fmt"

	"gitlab.paradise-soft.com.tw/dwh/dispatcher"
)

func main() {
	dispatcher.Subscribe("my-topic", "my-group-id", func(key, val string) {
		fmt.Printf("key: %v, val: %v\n", key, val)
	})
}
