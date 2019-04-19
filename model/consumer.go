package model

import (
	"github.com/Shopify/sarama"
)

type ConsumerCallback func(key, value []byte) error

type ConsumerCallbackError struct {
	Message *sarama.ConsumerMessage
	ErrStr  string
}
