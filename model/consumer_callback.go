package model

import "github.com/Shopify/sarama"

type BytesConsumerCallback func(value []byte) error

func (a BytesConsumerCallback) Wrap() (callback MessageConsumerCallback) {
	callback = func(message Message) error {
		return a(message.Value)
	}
	return
}

type MessageConsumerCallback func(message Message) error

type ConsumerCallbackError struct {
	Message *sarama.ConsumerMessage
	ErrStr  string
}
