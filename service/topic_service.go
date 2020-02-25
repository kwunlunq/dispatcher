package service

import (
	"github.com/pkg/errors"
	"gitlab.paradise-soft.com.tw/glob/dispatcher/glob"
	"gitlab.paradise-soft.com.tw/glob/dispatcher/model"
	"strconv"
	"sync"
	"time"

	"gitlab.paradise-soft.com.tw/glob/dispatcher/glob/core"

	"github.com/Shopify/sarama"
)

var TopicService = &topicService{lock: &sync.Mutex{}}

type topicService struct {
	createdTopics sync.Map
	lock          *sync.Mutex
}

func (s *topicService) Create(topic string) (err error) {
	if !s.checkExisted(topic) {
		s.lock.Lock()
		defer s.lock.Unlock()
		if !s.checkExisted(topic) {
			err = s.create(topic)
			if err != nil {
				err = errors.Wrap(err, "error creating topic")
				return
			}
			core.Logger.Debug("Topic created: ", topic)
			s.createdTopics.Store(topic, struct{}{})
		}
	}
	return nil
}

func (s *topicService) Remove(topics ...string) (err error) {
	if !core.IsInitialized() {
		return model.ErrNotInitialized
	}

	client, err := ClientService.Get()
	if err != nil {
		err = errors.Wrap(err, "remove topic error")
		return
	}
	broker, err := client.Controller()
	if err != nil {
		err = errors.Wrap(err, "remove topic error")
		return
	}
	_, err = broker.Connected()
	if err != nil {
		err = errors.Wrap(err, "remove topic error")
		return
	}
	request := &sarama.DeleteTopicsRequest{
		Timeout: time.Second * 30,
		Topics:  topics,
	}
	_, err = broker.DeleteTopics(request)
	if err != nil {
		err = errors.Wrap(err, "remove topic error")
		return
	}

	for _, topic := range topics {
		s.createdTopics.Delete(topic)
	}

	core.Logger.Debugf("Topics %v deleted", topics)
	return
}

func (s *topicService) checkExisted(topic string) (existed bool) {
	_, existed = s.createdTopics.Load(topic)
	return
}

func (s *topicService) List() (topics []string) {
	client, err := ClientService.Get()
	if err != nil {
		err = errors.Wrap(err, "list topics error")
		core.Logger.Error("List topic error:", err.Error())
		return
	}
	err = client.RefreshMetadata()
	if err != nil {
		err = errors.Wrap(err, "fail refresh metadata")
		core.Logger.Error("List topic error:", err.Error())
	}
	topics, _ = client.Topics()
	return
}

func (s *topicService) RemoveMapEntry(topic string) {
	s.createdTopics.Delete(topic)
}

func (s *topicService) create(topic string) (err error) {
	var broker *sarama.Broker
	client, err := ClientService.GetNew()
	if err != nil {
		err = errors.Wrap(err, "create topic error")
		return
	}
	broker, err = client.Controller()
	if err != nil {
		core.Logger.Errorf("Error retrieving broker: %v", err.Error())
		return
	}

	// check if the connection was OK
	_, err = broker.Connected()
	if err != nil {
		core.Logger.Errorf("Error connecting by broker: %v", err.Error())
		return err
	}

	// Setup the Topic details in CreateTopicRequest struct
	topicDetail := &sarama.TopicDetail{}
	topicDetail.NumPartitions = int32(core.Config.KafkaConfig.TopicPartitionNum)
	topicDetail.ReplicationFactor = int16(core.Config.KafkaConfig.TopicReplicationNum)
	topicDetail.ConfigEntries = make(map[string]*string)
	if core.Config.KafkaConfig.MinInsyncReplicas > 0 {
		topicDetail.ConfigEntries["min.insync.replicas"] = glob.StrToPtr(strconv.Itoa(core.Config.KafkaConfig.MinInsyncReplicas))
	}
	if core.Config.KafkaConfig.MsgMaxBytes > 0 {
		topicDetail.ConfigEntries["max.message.bytes"] = glob.StrToPtr(strconv.Itoa(core.Config.KafkaConfig.MsgMaxBytes))
	}

	topicDetails := make(map[string]*sarama.TopicDetail)
	topicDetails[topic] = topicDetail

	request := sarama.CreateTopicsRequest{
		Timeout:      time.Second * 15,
		TopicDetails: topicDetails,
	}

	// Send request to Broker
	var res *sarama.CreateTopicsResponse
	res, err = broker.CreateTopics(&request)
	if res != nil && len(res.TopicErrors) > 0 {
		for _, val := range res.TopicErrors {
			if val.Err != sarama.ErrNoError && val.Err != sarama.ErrTopicAlreadyExists {
				err = val.Err
				break
			}
		}
	}

	// handle errors if any
	if err != nil {
		// log.Printf("%#v", &err)
		err = errors.Wrapf(err, "err creating topic [%v]", topic)
		core.Logger.Errorf(err.Error())
		return
	}

	// close connection
	closingErr := broker.Close()
	if closingErr != nil {
		core.Logger.Error("Error closing broker on topic creation", closingErr.Error())
		return
	}
	closingErr = client.Close()
	if closingErr != nil {
		core.Logger.Error("Error closing client on topic creation", closingErr.Error())
		return
	}
	return nil
}
