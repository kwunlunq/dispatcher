package model

import (
	"github.com/Shopify/sarama"
	"gitlab.paradise-soft.com.tw/glob/dispatcher/glob"
	"gitlab.paradise-soft.com.tw/glob/dispatcher/glob/core"
	"log"
	"os"
	"time"
)

// Dispatcher defines all optional fields available to be customized.
type Dispatcher struct {
	// Init options
	KafkaConfig    core.KafkaConfig
	LogLevel       string
	DefaultGroupID string
	UserKey        string

	// Producer options
	ProducerErrHandler   ProducerCustomerErrHandler // handle error from consumer
	ProducerEnsureOrder  bool                       // promise meesges of the topic in order, automatically gen msg's key if enable
	ProducerMessageKey   string
	ProducerReplyHandler func(DispatcherMessage, error)
	ProducerReplyTimeout time.Duration

	// Consumer options
	ConsumerAsyncNum   int  // num of gorutine to process msg
	ConsumerOmitOldMsg bool // set kafka to OffsetNewest if enable
	ConsumerGroupID    string
}

func MakeDispatcher(opts []Option) Dispatcher {
	d := &Dispatcher{}
	d.WithOptions(opts)
	glob.SetIfZero(&d.KafkaConfig, "MsgMaxBytes", 20000000)
	glob.SetIfZero(&d.KafkaConfig, "TopicPartitionNum", 10)
	glob.SetIfZero(&d.KafkaConfig, "TopicReplicationNum", 3)
	glob.SetIfZero(&d.KafkaConfig, "MinInsyncReplicas", 3)
	glob.SetIfZero(d, "DefaultGroupID", glob.GenDefaultGroupID())
	glob.SetIfZero(d, "ConsumerAsyncNum", 1)
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
	//tmpC.Net.DialTimeout = timeout  // default 30s

	// Producer
	tmpC.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	tmpC.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	tmpC.Producer.Return.Successes = true          // Receive success msg
	tmpC.Producer.MaxMessageBytes = d.KafkaConfig.MsgMaxBytes
	tmpC.Producer.Timeout = 30 * time.Second // Default 10s
	//tmpC.Producer.Compression
	//tmpC.Producer.CompressionLevel

	// Consumer
	tmpC.Consumer.Return.Errors = true
	tmpC.Consumer.Offsets.Initial = sarama.OffsetOldest    // OffsetNewest,Oldest
	tmpC.Consumer.Group.Session.Timeout = 30 * time.Second // Default 10s
	//tmpC.Consumer.Group.Heartbeat = timeout              // Default 3s, should be set to lower than 1/3 of Consumer.Group.Session.Timeout
	//tmpC.Net.DialTimeout = timeout  // default 30s
	//tmpC.Net.ReadTimeout = timeout  // default 30s
	//tmpC.Net.WriteTimeout = timeout // default 30s

	// TLS
	// tlsConfig := createTlsConfiguration()
	// if tlsConfig != nil {
	// 	tmpC.Net.TLS.CoreConfig = tlsConfig
	// 	tmpC.Net.TLS.Enable = true
	// }

	tmpC.ClientID = "dispatcher"

	// Switch on sarama log if needed
	if d.LogLevel == "debug" {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.Ltime)
	}
	//sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)

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
