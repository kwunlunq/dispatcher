package model

type ProducerErrHandler func(key, value []byte, err error)
