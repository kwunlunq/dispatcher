package model

type ProducerCustomerErrHandler func(key, value []byte, err error)
