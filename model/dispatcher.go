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
	KafkaConfig     core.KafkaConfig
	LogLevel        string
	EnableSaramaLog bool
	DefaultGroupID  string
	UserKey         string
	MonitorHost     string

	// Producer options
	ProducerErrHandler   ProducerCustomerErrHandler // handle error from consumer
	ProducerEnsureOrder  bool                       // promise messages of the topic in order, automatically gen msg's key if enable
	ProducerMessageKey   string
	ProducerReplyHandler func(Message, error)
	ProducerReplyTimeout time.Duration

	// Consumer options
	ConsumerAsyncNum              int  // num of goroutine to process msg
	ConsumerOmitOldMsg            bool // set kafka to OffsetNewest if enable
	ConsumerGroupID               string
	ConsumerLagCountHandler       func(lagCount int)
	ConsumerLagCountInterval      time.Duration
	ConsumerIsStopOnCallbackError bool
}

func MakeDispatcher(opts []Option) Dispatcher {
	d := &Dispatcher{
		ConsumerIsStopOnCallbackError: false,
	}
	d.WithOptions(opts)
	glob.SetIfZero(&d.KafkaConfig, "MsgMaxBytes", 20000000)
	glob.SetIfZero(&d.KafkaConfig, "TopicPartitionNum", 10)
	glob.SetIfZero(&d.KafkaConfig, "TopicReplicationNum", 3)
	glob.SetIfZero(&d.KafkaConfig, "MinInsyncReplicas", 3)
	glob.SetIfZero(d, "DefaultGroupID", glob.GenDefaultGroupID())
	glob.SetIfZero(d, "ConsumerAsyncNum", 1)
	glob.SetIfZero(d, "ConsumerLagCountInterval", int64(time.Duration(10*time.Second)))
	glob.SetIfZero(d, "LogLevel", "error")
	return *d
}
func (d Dispatcher) ToCoreConfig() core.CoreConfig {
	return core.CoreConfig{
		DefaultGroupID: d.DefaultGroupID,
		KafkaConfig:    d.KafkaConfig,
		MonitorHost:    d.MonitorHost,
	}
}

func (d Dispatcher) ToSaramaConfig() (config *sarama.Config) {
	config = sarama.NewConfig()
	config.Version = sarama.V2_1_0_0 // To enable consumer group, but will cause disable of 'auto.create.topic'

	config.Admin.Timeout = 10 * time.Second // default 3s

	// Net
	config.Net.DialTimeout = 10 * time.Second // default 30s

	// Metadata
	config.Metadata.Retry.Max = 5                          // default 3
	config.Metadata.Retry.Backoff = 500 * time.Millisecond // default 250ms
	config.Metadata.RefreshFrequency = time.Minute         // default 10m

	// Producer
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	config.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	config.Producer.Return.Successes = true          // Receive success msg
	config.Producer.MaxMessageBytes = d.KafkaConfig.MsgMaxBytes
	config.Producer.Timeout = 30 * time.Second // Default 10s
	config.Producer.Compression = sarama.CompressionLZ4
	//config.Producer.CompressionLevel = 6

	// Consumer
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest    // OffsetNewest,Oldest
	config.Consumer.Group.Session.Timeout = 30 * time.Second // Default 10s
	//config.Consumer.Group.Heartbeat = timeout              // Default 3s, should be set to lower than 1/3 of Consumer.Group.Session.Timeout
	//config.Net.DialTimeout = timeout  // default 30s
	//config.Net.ReadTimeout = timeout  // default 30s
	//config.Net.WriteTimeout = timeout // default 30s

	// TODO: 支援TLS
	// TLS
	// tlsConfig := createTlsConfiguration()
	// if tlsConfig != nil {
	// 	config.Net.TLS.CoreConfig = tlsConfig
	// 	config.Net.TLS.Enable = true
	// }

	config.ClientID = "dispatcher"

	// Switch on sarama log if needed
	if d.EnableSaramaLog {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.Ltime)
	}
	//sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)

	return config
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
