package model

import (
	"gitlab.paradise-soft.com.tw/dwh/dispatcher/glob"
)

// Dispatcher defines all optional fields available to be customized.
type Dispatcher struct {
	// Common options
	MsgMaxBytes         int    `mapstructure:"msg_max_bytes"         json:"msg_max_bytes"`
	TopicPartitionNum   int    `mapstructure:"topic_partition_num"   json:"topic_partition_num"`
	TopicReplicationNum int    `mapstructure:"topic_replication_num" json:"topic_replication_num"`
	LogLevel            string `mapstructure:"log_level"             json:"log_level"`

	// Producer options
	ProducerErrHandler  ProducerCustomerErrHandler // handle error from consumer
	ProducerEnsureOrder bool                       // promise meesges of the topic in order, automatically gen msg's key if enable

	// Consumer options
	ConsumerAsyncNum   int  // num of gorutine to process msg
	ConsumerOmitOldMsg bool // set kafka to OffsetNewest if enable
}

func MakeDispatcher(opts []Option) Dispatcher {
	d := &Dispatcher{}
	d.WithOptions(opts)
	glob.SetIfZero(d, "TopicPartitionNum", 10)
	glob.SetIfZero(d, "TopicReplicationNum", 2)
	glob.SetIfZero(d, "MsgMaxBytes", 20000000)
	return *d
}

type Option interface {
	apply(*Dispatcher)
}

type FuncOption func(*Dispatcher)

func (f FuncOption) apply(d *Dispatcher) {
	f(d)
}

func (d *Dispatcher) WithOptions(opts []Option) *Dispatcher {
	for _, opt := range opts {
		opt.apply(d)
	}
	return d
}

func (d *Dispatcher) CopyWithOptions(opts []Option) *Dispatcher {
	copy := d.clone()
	for _, opt := range opts {
		opt.apply(copy)
	}
	return copy
}

func (d *Dispatcher) clone() *Dispatcher {
	copy := *d
	return &copy
}