package model

import "github.com/Shopify/sarama"

type BytesConsumerCallback func(value []byte) error

func (a BytesConsumerCallback) Wrap() (callback DispatcherMessageConsumerCallback) {
	callback = func(message DispatcherMessage) error {
		return a(message.Value)
	}
	return
}

type DispatcherMessageConsumerCallback func(message DispatcherMessage) error

type ConsumerCallbackError struct {
	Message *sarama.ConsumerMessage
	ErrStr  string
}
