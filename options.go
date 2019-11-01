package dispatcher

import (
	"gitlab.paradise-soft.com.tw/glob/dispatcher/glob/core"
	"gitlab.paradise-soft.com.tw/glob/dispatcher/model"
	"time"
)

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

// ProducerSetMessageKey set the key for each message
func ProducerSetMessageKey(key string) model.Option {
	return model.FuncOption(func(d *model.Dispatcher) { d.ProducerMessageKey = key })
}

// ProducerSetReplyHandler collect reply message from consumer along with transmission related information.
func ProducerCollectReplyMessage(retryHandler func(Message, error), timeout time.Duration) model.Option {
	return model.FuncOption(func(d *model.Dispatcher) {
		d.ProducerReplyHandler = func(message model.Message, err error) { retryHandler(APIMessageFromMessage(message), err) }
		d.ProducerReplyTimeout = timeout
	})
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

// InitSetKafkaConfig set kafka related configs.
func InitSetKafkaConfig(config KafkaConfig) model.Option {
	return model.FuncOption(func(d *model.Dispatcher) { d.KafkaConfig = core.KafkaConfig(config) })
}

func InitSetDefaultGroupID(defaultGroupID string) model.Option {
	return model.FuncOption(func(d *model.Dispatcher) { d.DefaultGroupID = defaultGroupID })
}

// InitSetLogLevel set log level of log in dispatcher, available levels are debug, info, warn, error.
func InitSetLogLevel(logLevel string) model.Option {
	return model.FuncOption(func(d *model.Dispatcher) { d.LogLevel = logLevel })
}
