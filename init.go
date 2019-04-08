package dispatcher

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"log"
	"strings"

	"github.com/Shopify/sarama"
	"gitlab.paradise-soft.com.tw/backend/yaitoo/cfg"
)

var (
	appConfig    *cfg.Config
	saramaConfig *sarama.Config
	ipList       string
	brokerList   []string
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
	confKey = "dispatcher"
)

func init() {
	loadCommonConfigs()
	loadProducerConfigs()
	loadConsumerConfigs()
	loadSaramaConfigs()
}

func loadSaramaConfigs() {

	c := sarama.NewConfig()

	c.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	c.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	c.Producer.Return.Successes = true

	c.Version = sarama.V2_1_0_0
	c.Consumer.Return.Errors = true

	tlsConfig := createTlsConfiguration()
	if tlsConfig != nil {
		c.Net.TLS.Config = tlsConfig
		c.Net.TLS.Enable = true
	}

	saramaConfig = c
}

func loadCommonConfigs() {
	appConfig = cfg.Load("app.conf")
	ipList = appConfig.GetValue(confKey, "ip_list", "127.0.0.1")
	brokerList = strings.Split(ipList, ",")
	log.Printf("Kafka brokers: %s", strings.Join(brokerList, ", "))
}

func loadProducerConfigs() {
}

func loadConsumerConfigs() {
}

func createTlsConfiguration() (t *tls.Config) {
	tlsEnable := appConfig.GetValueAsBool(confKey, "tls_enable", false)
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
