package glob

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"log"
	"strings"

	"gitlab.paradise-soft.com.tw/glob/common/settings"

	"github.com/Shopify/sarama"
	"gitlab.paradise-soft.com.tw/backend/yaitoo/cfg"
	"gitlab.paradise-soft.com.tw/backend/yaitoo/tracer"
)

var (
	// Public
	SaramaConfig *sarama.Config
	BrokerList   []string
	Topic        string

	appConfig *cfg.Config
	ipList    string

	// Producer
	addr      *string // = flag.String("addr", ":8080", "The address to bind to")
	brokers   *string // = flag.String("brokers", "10.200.252.180:9092,10.200.252.181:9092,10.200.252.182:9092,10.200.252.183:9092", "The Kafka brokers to connect to, as a comma separated list")
	verbose   *bool   // = flag.Bool("verbose", true, "Turn on Sarama logging")
	certFile  *string // = flag.String("certificate", "", "The optional certificate file for client authentication")
	keyFile   *string // = flag.String("key", "", "The optional key file for client authentication")
	caFile    *string // = flag.String("ca", "", "The optional certificate authority file for TLS client authentication")
	verifySsl *bool   // = flag.Bool("verify", false, "Optional verify ssl certificates chain")
	// Consumer
)

const (
	ProjName = "dispatcher"
)

func init() {
	loadCommonConfigs()
	loadProducerConfigs()
	loadConsumerConfigs()
	loadSaramaConfigs()
}

func loadSaramaConfigs() {

	// sarama.Logger = log.New(os.Stdout, "", log.Ltime)

	c := sarama.NewConfig()

	c.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	c.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	// c.Producer.Return.Successes = true       // Receive success msg

	c.Version = sarama.V2_1_0_0 // To enable consumer group
	// c.Version = "v2.1.0" // To enable consumer group
	c.Consumer.Return.Errors = true
	// c.Consumer.Offsets.Initial = sarama.OffsetOldest

	tlsConfig := createTlsConfiguration()
	if tlsConfig != nil {
		c.Net.TLS.Config = tlsConfig
		c.Net.TLS.Enable = true
	}

	SaramaConfig = c
}

func loadCommonConfigs() {
	// appConfig = cfg.Load("app.conf")
	appConfig = settings.Config
	ipList = appConfig.GetValue(ProjName, "brokers", "127.0.0.1")
	BrokerList = strings.Split(ipList, ",")
	tracer.Tracef(ProjName, " Kafka brokers: %v", strings.Join(BrokerList, ", "))
	Topic = appConfig.GetValue(ProjName, "topic", "my-topic")
}

func loadProducerConfigs() {
}

func loadConsumerConfigs() {
}

func createTlsConfiguration() (t *tls.Config) {
	tlsEnable := appConfig.GetValueAsBool(ProjName, " tls_enable", false)
	if !tlsEnable {
		return nil
	}

	if *certFile != "" && *keyFile != "" && *caFile != "" {
		cert, err := tls.LoadX509KeyPair(*certFile, *keyFile)
		if err != nil {
			log.Fatal(err)
		}

		caCert, err := ioutil.ReadFile(*caFile)
		if err != nil {
			log.Fatal(err)
		}

		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)

		t = &tls.Config{
			Certificates:       []tls.Certificate{cert},
			RootCAs:            caCertPool,
			InsecureSkipVerify: *verifySsl,
		}
	}
	// will be nil by default if nothing is provided
	return t
}
