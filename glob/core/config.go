package core

import (
	"log"
	"time"

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
}

type KafkaConfig struct {
	TopicPartitionNum   int
	TopicReplicationNum int
	MinInsyncReplicas   int
	MsgMaxBytes         int
	Timeout             time.Duration
}

func InitConfig(brokers []string, c CoreConfig) {
	Config = c
	Config.Brokers = brokers
	Config.IsInitialized = true
}

func InitSaramaConfig(tmpC *sarama.Config) {
	SaramaConfig = *tmpC
}
func IsInitialized() bool {
	if !Config.IsInitialized {
		log.Println("dispatcher hasn't initialized!")
	}
	return Config.IsInitialized
}

//func initConfig(brokers []string, dis model.Dispatcher) {
//	Config = config{
//		Brokers:             brokers,
//		DefaultGroupID:      dis.DefaultGroupID,
//		TopicPartitionNum:   dis.TopicPartitionNum,
//		TopicReplicationNum: dis.TopicReplicationNum,
//	}
//}

//func initSaramaConfig(dis model.Dispatcher) {
//
//	tmpC := sarama.NewConfig()
//	tmpC.Version = sarama.V2_1_0_0 // To enable consumer group, but will cause disable of 'auto.create.topic'
//
//	// Producer
//	tmpC.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
//	tmpC.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
//	tmpC.Producer.Return.Successes = true          // Receive success msg
//	tmpC.Producer.MaxMessageBytes = dis.MsgMaxBytes
//
//	// Consumer
//	tmpC.Consumer.Return.Errors = true
//	tmpC.Consumer.Offsets.Initial = sarama.OffsetOldest // OffsetNewest,Oldest
//	tmpC.Net.ReadTimeout = 300 * time.Second            // Other timeout: Consumer.Group.Session.Timeout, Net.DialTimeout, Net.WriteTimeout, Net.WriteTimeout
//
//	// TLS
//	// tlsConfig := createTlsConfiguration()
//	// if tlsConfig != nil {
//	// 	tmpC.Net.TLS.Config = tlsConfig
//	// 	tmpC.Net.TLS.Enable = true
//	// }
//
//	tmpC.ClientID = "dispatcher"
//
//	// Switch on sarama log if needed
//	// sarama.Logger = log.New(os.Stdout, "[sarama] ", log.Ltime)
//	// sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
//
//	SaramaConfig = *tmpC
//}

/* TODO: 待整理
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
