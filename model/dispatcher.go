package model

import (
	"github.com/Shopify/sarama"
	"gitlab.paradise-soft.com.tw/glob/dispatcher/glob"
	"gitlab.paradise-soft.com.tw/glob/dispatcher/glob/core"
)

// Dispatcher defines all optional fields available to be customized.
type Dispatcher struct {
	// Init options
	KafkaConfig    core.KafkaConfig
	LogLevel       string
	DefaultGroupID string

	// Producer options
	ProducerErrHandler  ProducerCustomerErrHandler // handle error from consumer
	ProducerEnsureOrder bool                       // promise meesges of the topic in order, automatically gen msg's key if enable

	// Consumer options
	ConsumerAsyncNum   int  // num of gorutine to process msg
	ConsumerOmitOldMsg bool // set kafka to OffsetNewest if enable
	ConsumerGroupID    string
}

func MakeDispatcher(opts []Option) Dispatcher {
	d := &Dispatcher{
		DefaultGroupID:   glob.GetHashMacAddrs(),
		ConsumerAsyncNum: 1,
	}
	d.WithOptions(opts)
	glob.SetIfZero(&d.KafkaConfig, "MsgMaxBytes", 20000000)
	glob.SetIfZero(&d.KafkaConfig, "TopicPartitionNum", 10)
	glob.SetIfZero(&d.KafkaConfig, "TopicReplicationNum", 3)
	glob.SetIfZero(&d.KafkaConfig, "MinInsyncReplicas", 3)
	//glob.SetIfZero(&d.KafkaConfig, "Timeout", int64(60*time.Second))
	return *d
}
func (d Dispatcher) ToCoreConfig() core.CoreConfig {
	return core.CoreConfig{
		DefaultGroupID: d.DefaultGroupID,
		KafkaConfig:    d.KafkaConfig,
	}
}

func (d Dispatcher) ToSaramaConfig() *sarama.Config {
	tmpC := sarama.NewConfig()
	tmpC.Version = sarama.V2_1_0_0 // To enable consumer group, but will cause disable of 'auto.create.topic'

	//timeout := d.KafkaConfig.Timeout

	// Net
	//tmpC.Net.DialTimeout = timeout

	// Producer
	tmpC.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	tmpC.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	tmpC.Producer.Return.Successes = true          // Receive success msg
	tmpC.Producer.MaxMessageBytes = d.KafkaConfig.MsgMaxBytes
	//tmpC.Producer.Timeout = timeout
	//tmpC.Producer.Compression
	//tmpC.Producer.CompressionLevel

	// Consumer
	tmpC.Consumer.Return.Errors = true
	tmpC.Consumer.Offsets.Initial = sarama.OffsetOldest // OffsetNewest,Oldest
	//tmpC.Consumer.Group.Session.Timeout = timeout
	//tmpC.Net.DialTimeout = timeout
	//tmpC.Net.ReadTimeout = timeout
	//tmpC.Net.WriteTimeout = timeout

	// TLS
	// tlsConfig := createTlsConfiguration()
	// if tlsConfig != nil {
	// 	tmpC.Net.TLS.CoreConfig = tlsConfig
	// 	tmpC.Net.TLS.Enable = true
	// }

	tmpC.ClientID = "dispatcher"

	// Switch on sarama log if needed
	// sarama.Logger = log.New(os.Stdout, "[sarama] ", log.Ltime)
	// sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)

	return tmpC
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
	copied := d.clone()
	for _, opt := range opts {
		opt.apply(copied)
	}
	return copied
}

func (d *Dispatcher) clone() *Dispatcher {
	copied := *d
	return &copied
}
