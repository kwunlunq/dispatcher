package glob

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"log"
	"strings"

	"github.com/google/uuid"

	"gitlab.paradise-soft.com.tw/glob/common/settings"

	"github.com/Shopify/sarama"
	"gitlab.paradise-soft.com.tw/backend/yaitoo/cfg"
	"gitlab.paradise-soft.com.tw/backend/yaitoo/tracer"
)

var Config config

const ProjName = "dispatcher"

func init() {
	Config = newConfig()
}

func newConfig() config {
	c := &config{}
	c.appConfig = settings.Config
	c.loadSaramaConfigs()
	return *c
}

type config struct {
	// Public
	Topic      string
	BrokerList []string
	GroupID    string

	// Topic
	TopicPartitionNum   int
	TopicReplicationNum int

	SaramaConfig sarama.Config
	appConfig    *cfg.Config

	// Message
	msgMaxBytes int

	// Producer
	addr      *string // = flag.String("addr", ":8080", "The address to bind to")
	brokers   *string // = flag.String("brokers", "10.200.252.180:9092,10.200.252.181:9092,10.200.252.182:9092,10.200.252.183:9092", "The Kafka brokers to connect to, as a comma separated list")
	verbose   *bool   // = flag.Bool("verbose", true, "Turn on Sarama logging")
	certFile  *string // = flag.String("certificate", "", "The optional certificate file for client authentication")
	keyFile   *string // = flag.String("key", "", "The optional key file for client authentication")
	caFile    *string // = flag.String("ca", "", "The optional certificate authority file for TLS client authentication")
	verifySsl *bool   // = flag.Bool("verify", false, "Optional verify ssl certificates chain")
	// Consumer
}

func (c *config) loadSaramaConfigs() {

	c.Topic = c.appConfig.GetValue(ProjName, "topic", "my-topic")
	c.GroupID = c.appConfig.GetValue(ProjName, "group_id", uuid.New().String())

	brokers := c.appConfig.GetValue(ProjName, "brokers", "127.0.0.1")
	c.BrokerList = strings.Split(brokers, ",")
	tracer.Infof(ProjName, " Kafka brokers: %v", strings.Join(c.BrokerList, ", "))

	c.TopicPartitionNum = c.appConfig.GetValueAsInt(ProjName, "topic_partition_num", 10)
	c.TopicReplicationNum = c.appConfig.GetValueAsInt(ProjName, "topic_replication_num", 2)
	c.msgMaxBytes = c.appConfig.GetValueAsInt(ProjName, "msg_max_bytes", 20000000) // 20M

	// sarama.Logger = log.New(os.Stdout, "[sarama] ", log.Ltime)
	// sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)

	tmpC := sarama.NewConfig()
	tmpC.Version = sarama.V2_1_0_0 // To enable consumer group, but will cause disable of 'auto.create.topic'

	// Producer
	tmpC.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	tmpC.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	tmpC.Producer.Return.Successes = true          // Receive success msg
	tmpC.Producer.MaxMessageBytes = c.msgMaxBytes

	// Consumer
	tmpC.Consumer.Return.Errors = true
	// tmpC.Consumer.Offsets.Initial = sarama.OffsetNewest
	tmpC.Consumer.Offsets.Initial = sarama.OffsetOldest

	// TLS
	tlsConfig := c.createTlsConfiguration()
	if tlsConfig != nil {
		tmpC.Net.TLS.Config = tlsConfig
		tmpC.Net.TLS.Enable = true
	}

	tmpC.ClientID = "dispatcher"

	c.SaramaConfig = *tmpC
}

func (c *config) createTlsConfiguration() (t *tls.Config) {
	tlsEnable := c.appConfig.GetValueAsBool(ProjName, " tls_enable", false)
	if !tlsEnable {
		return nil
	}

	if *c.certFile != "" && *c.keyFile != "" && *c.caFile != "" {
		cert, err := tls.LoadX509KeyPair(*c.certFile, *c.keyFile)
		if err != nil {
			log.Fatal(err)
		}

		caCert, err := ioutil.ReadFile(*c.caFile)
		if err != nil {
			log.Fatal(err)
		}

		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)

		t = &tls.Config{
			Certificates:       []tls.Certificate{cert},
			RootCAs:            caCertPool,
			InsecureSkipVerify: *c.verifySsl,
		}
	}
	// will be nil by default if nothing is provided
	return t
}
