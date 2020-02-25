package core

import (
	"github.com/Shopify/sarama"
)

const ProjectName = "dispatcher"

var (
	Config       CoreConfig
	SaramaConfig sarama.Config
)

type CoreConfig struct {
	KafkaConfig    KafkaConfig
	DefaultGroupID string
	Brokers        []string
	IsInitialized  bool
	UserKey        string
	MonitorHost    string
}

type KafkaConfig struct {
	TopicPartitionNum   int
	TopicReplicationNum int
	MinInsyncReplicas   int
	MsgMaxBytes         int
	//Timeout             time.Duration `json:"-"`
}

func InitConfig(brokers []string, c CoreConfig) {
	Config = c
	Config.Brokers = brokers
	Config.IsInitialized = true
	//Config.UserKey = userKey
}

func InitSaramaConfig(tmpC *sarama.Config) {
	SaramaConfig = *tmpC
}
func IsInitialized() bool {
	return Config.IsInitialized
}

// TODO: TLS連線
/*
func createTlsConfiguration() (t *tls.Config) {
	tlsEnable := Config.TLSEnable
	if !tlsEnable {
		return nil
	}

	if Config.CertFile != "" && Config.KeyFile != "" && Config.CaFile != "" {
		cert, err := tls.LoadX509KeyPair(Config.CertFile, Config.KeyFile)
		if err != nil {
			log.Fatal(err)
		}

		caCert, err := ioutil.ReadFile(Config.CaFile)
		if err != nil {
			log.Fatal(err)
		}

		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)

		t = &tls.Config{
			Certificates:       []tls.Certificate{cert},
			RootCAs:            caCertPool,
			InsecureSkipVerify: Config.VerifySsl,
		}
	}
	// will be nil by default if nothing is provided
	return t
}
*/
