package model

import "errors"

// Define errors in dispatcher
var (
	ErrNotInitialized        = errors.New("dispatcher hasn't initialized")
	ErrSubscribeExistedTopic = errors.New("subscribing on subscribed topic")
)
