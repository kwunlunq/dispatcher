package service

import (
	"sync"
	"time"

	"gitlab.paradise-soft.com.tw/glob/dispatcher/glob/core"

	"github.com/Shopify/sarama"
)

var TopicService = &topicService{lock: &sync.Mutex{}}

type topicService struct {
	topics []string
	lock   *sync.Mutex
}

func (s *topicService) Create(topic string) (err error) {
	if !s.checkExisted(topic) {
		s.lock.Lock()
		if !s.checkExisted(topic) {
			s.create(topic)
			s.topics = append(s.topics, topic)
		}
		s.lock.Unlock()
	}
	return nil
}

func (s *topicService) Remove(topics ...string) {

	broker, err := ClientService.Get().Controller()
	if err != nil {
		core.Logger.Errorf("Error retrieving broker: %v", err.Error())
		// return err
	}
	_, err = broker.Connected()
	if err != nil {
		core.Logger.Errorf("Error connecting by broker: %v", err.Error())
		// return err
	}
	request := &sarama.DeleteTopicsRequest{
		Timeout: time.Second * 15,
		Topics:  topics,
	}
	_, err = broker.DeleteTopics(request)
	if err != nil {
		core.Logger.Errorf("Error deleting topic: %v", err.Error())
	}

	core.Logger.Debugf("Topics %v deleted", topics)
	return
}

func (s *topicService) checkExisted(topic string) (existed bool) {
	for _, t := range s.topics {
		if t == topic {
			return true
		}
	}
	return false
}

func (s *topicService) List() (topics []string) {
	ClientService.Get().RefreshMetadata()
	topics, _ = ClientService.Get().Topics()
	return
}

func (s *topicService) create(topic string) (err error) {
	var broker *sarama.Broker
	broker, err = ClientService.Get().Controller()
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
	topicDetail.NumPartitions = int32(core.Config.TopicPartitionNum)
	topicDetail.ReplicationFactor = int16(core.Config.TopicReplicationNum)
	topicDetail.ConfigEntries = make(map[string]*string)

	topicDetails := make(map[string]*sarama.TopicDetail)
	topicDetails[topic] = topicDetail

	request := sarama.CreateTopicsRequest{
		Timeout:      time.Second * 15,
		TopicDetails: topicDetails,
	}

	// Send request to Broker
	_, err = broker.CreateTopics(&request)

	// handle errors if any
	if err != nil {
		// log.Printf("%#v", &err)
		core.Logger.Errorf("Error creating topic: %v", err.Error())
		return
	}
	core.Logger.Debugf("Topic created: %v.", topic)

	// close connection to broker
	// broker.Close()
	return nil
}
