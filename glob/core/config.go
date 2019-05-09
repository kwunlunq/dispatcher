package core

import (
	"log"
	"os"
	"time"

	"github.com/Shopify/sarama"
	"github.com/google/uuid"
	"gitlab.paradise-soft.com.tw/dwh/dispatcher/model"
)

var (
	Config       config
	SaramaConfig sarama.Config
)

type config struct {
	Brokers             []string
	GroupID             string
	TopicPartitionNum   int
	TopicReplicationNum int
	isInitialized       bool
}

func initConfig(brokers []string, groupID string, dis model.Dispatcher) {
	if groupID == "" {
		groupID = uuid.New().String()
	}
	Config = config{
		Brokers:             brokers,
		GroupID:             groupID,
		TopicPartitionNum:   dis.TopicPartitionNum,
		TopicReplicationNum: dis.TopicReplicationNum,
	}
}

func initSaramaConfig(dis model.Dispatcher) {

	tmpC := sarama.NewConfig()
	tmpC.Version = sarama.V2_1_0_0 // To enable consumer group, but will cause disable of 'auto.create.topic'

	// Producer
	tmpC.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	tmpC.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	tmpC.Producer.Return.Successes = true          // Receive success msg
	tmpC.Producer.MaxMessageBytes = dis.MsgMaxBytes

	// Consumer
	tmpC.Consumer.Return.Errors = true
	tmpC.Consumer.Offsets.Initial = sarama.OffsetOldest // OffsetNewest,Oldest
	tmpC.Net.ReadTimeout = 300 * time.Second            // Other timeout: Consumer.Group.Session.Timeout, Net.DialTimeout, Net.WriteTimeout, Net.WriteTimeout

	// TLS
	// tlsConfig := createTlsConfiguration()
	// if tlsConfig != nil {
	// 	tmpC.Net.TLS.Config = tlsConfig
	// 	tmpC.Net.TLS.Enable = true
	// }

	tmpC.ClientID = "dispatcher"

	// Switch on sarama log if needed
	// sarama.Logger = log.New(os.Stdout, "[sarama] ", log.Ltime)
	// sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)

	SaramaConfig = *tmpC
}

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

func getWorkingDir() string {
	dir, err := os.Getwd()
	if err != nil {
		log.Printf(" Err getting working dir: %v\n", err)
	}
	return dir
}

/*
type config struct {
	// kafka
	Brokers             []string `mapstructure:"brokers"`
	TopicPartitionNum   int      `mapstructure:"topic_partition_num"`
	TopicReplicationNum int      `mapstructure:"topic_replication_num"`
	MsgMaxBytes         int      `mapstructure:"msg_max_bytes"`

	// consumer
	GroupID string `mapstructure:"group_id"`

	// tls
	TLSEnable bool   `mapstructure:"tls_enable"`
	VerifySsl bool   `mapstructure:"verifySsl"`
	CertFile  string `mapstructure:"cert_file"`
	KeyFile   string `mapstructure:"key_file"`
	CaFile    string `mapstructure:"ca_file"`

	// testing
	TestCount int    `mapstructure:"testCount"`
	Topic     string `mapstructure:"topic"`
	LogLevel  string `mapstructure:"log_level"`
}
*/

// func loadConf() {
// 	readConfigFile()
// 	initValue()
// 	initSaramaConfig()
// }

/*
func readConfigFile() {
	// First read app.xxx, with extension supported by viper.
	viper.AddConfigPath(".")
	viper.SetConfigName("app")
	err := viper.ReadInConfig()
	if err != nil {
		// Then read app.conf if any err occurred
		log.Printf("Err reading conf file: %v\n", err.Error())
		viper.SetConfigFile("app.conf")
		viper.SetConfigType("toml")
		err = viper.ReadInConfig()
		if err != nil {
			log.Printf("Err reading conf file: %v\n", err.Error())
		}
	}
	log.Printf("Config created with [%v]\n", viper.ConfigFileUsed())
	viper.UnmarshalKey("dispatcher", &Config)
}
*/
