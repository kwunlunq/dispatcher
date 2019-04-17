package model

type ConsumerCallback func(key, value []byte) error
