package dispatcher

import "gitlab.paradise-soft.com.tw/glob/dispatcher/model"

/*
 * Producer Options
 */

// ProducerAddErrHandler add handler for Producer to handle error received from consumer
func ProducerAddErrHandler(errHandler model.ProducerCustomerErrHandler) model.Option {
	return model.FuncOption(func(d *model.Dispatcher) { d.ProducerErrHandler = errHandler })
}

// ProducerEnsureOrder ensures messages keep order as same as it was when being produced.
func ProducerEnsureOrder() model.Option {
	return model.FuncOption(func(d *model.Dispatcher) { d.ProducerEnsureOrder = true })
}

/*
 * Consumer Options
 */

// ConsumerSetAsyncNum set gorutine num to process messages on subscribing.
func ConsumerSetAsyncNum(num int) model.Option {
	return model.FuncOption(func(d *model.Dispatcher) { d.ConsumerAsyncNum = num })
}

// ConsumerOmitOldMsg omit old message if set to true.
func ConsumerOmitOldMsg() model.Option {
	return model.FuncOption(func(d *model.Dispatcher) { d.ConsumerOmitOldMsg = true })
}

// ConsumerSetGroupID set the GroupID, overriding the ont set in Init.
func ConsumerSetGroupID(groupID string) model.Option {
	return model.FuncOption(func(d *model.Dispatcher) { d.ConsumerGroupID = groupID })
}

/*
 * Init Options
 */

// InitSetMsgMaxBytes ...
func InitSetMsgMaxBytes(msgMaxBytes int) model.Option {
	return model.FuncOption(func(d *model.Dispatcher) { d.MsgMaxBytes = msgMaxBytes })
}

// InitSetLogLevel set log level of log in diapatcher, available levels are debug, info, warn, error.
func InitSetLogLevel(logLevel string) model.Option {
	return model.FuncOption(func(d *model.Dispatcher) { d.LogLevel = logLevel })
}

// InitSetPartitionNum set the partition num to use on creating topic.
func InitSetPartitionNum(partitionNum int) model.Option {
	return model.FuncOption(func(d *model.Dispatcher) { d.TopicPartitionNum = partitionNum })
}

// InitSetReplicaNum set replica num of messages sent.
func InitSetReplicaNum(replicaNum int) model.Option {
	return model.FuncOption(func(d *model.Dispatcher) { d.TopicReplicationNum = replicaNum })
}
