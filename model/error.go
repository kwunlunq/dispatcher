package model

import "errors"

// Define errors in dispatcher
var (
	ErrNotInitialized             = errors.New("dispatcher not initialized")
	ErrSubscribeOnSubscribedTopic = errors.New("subscribing on subscribed topic")
	ErrTimeout                    = errors.New("timeout waiting result")
	ErrConsumerStoppedManually    = errors.New("consumer stopped manually")
)
