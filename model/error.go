package model

import "errors"

// Define errors in dispatcher
var (
	ErrNotInitialized             = errors.New("dispatcher hasn't initialized")
	ErrSubscribeOnSubscribedTopic = errors.New("subscribing on subscribed topic")
	ErrConsumeStopWithoutError    = errors.New("subscribe terminated without error")
	ErrTimeout                    = errors.New("timeout waiting result")
)
