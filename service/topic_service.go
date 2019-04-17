package service

import (
	"time"

	"gitlab.paradise-soft.com.tw/backend/yaitoo/tracer"
	"gitlab.paradise-soft.com.tw/dwh/dispatcher/glob"

	"github.com/Shopify/sarama"
)

var TopicService = topicService{}

type topicService struct {
	topics []string
}

func (s topicService) Create(topic string) (err error) {
	if s.checkExisted(topic) {
		return nil
	}
	return s.create(topic)
}

func (s topicService) Remove(topics ...string) (err error) {

	broker, err := ClientService.Get().Controller()
	if err != nil {
		tracer.Errorf(glob.ProjName, "Error retrieving broker: %v", err.Error())
		return err
	}
	_, err = broker.Connected()
	if err != nil {
		tracer.Errorf(glob.ProjName, "Error connecting by broker: %v", err.Error())
		return err
	}
	request := &sarama.DeleteTopicsRequest{
		Timeout: time.Second * 15,
		Topics:  topics,
	}
	_, err = broker.DeleteTopics(request)
	if err != nil {
		tracer.Errorf(glob.ProjName, "Error deleting topic: %v", err.Error())
	}

	tracer.Tracef(glob.ProjName, "Topics %v deleted", topics)
	return
}

func (s topicService) checkExisted(topic string) (existed bool) {
	for _, t := range s.topics {
		if t == topic {
			return true
		}
	}
	return false
	// topics, err := ClientService.Get().Topics()
	// if err != nil {
	// 	tracer.Errorf(glob.ProjName, "Error getting topics: %v", err.Error())
	// 	return false, err
	// }
	// tracer.Tracef(glob.ProjName, "Existing topics: %v", topics)

	// log.Println(ClientService.Get().RefreshMetadata())
	// topics, err = ClientService.Get().Topics()
	// log.Println(topics, err)
	// return
}

func (s topicService) create(topic string) (err error) {
	var broker *sarama.Broker
	broker, err = ClientService.Get().Controller()
	if err != nil {
		tracer.Errorf(glob.ProjName, "Error retrieving broker: %v", err.Error())
		return
	}

	// check if the connection was OK
	_, err = broker.Connected()
	if err != nil {
		tracer.Errorf(glob.ProjName, "Error connecting by broker: %v", err.Error())
		return err
	}

	// Setup the Topic details in CreateTopicRequest struct
	topicDetail := &sarama.TopicDetail{}
	topicDetail.NumPartitions = int32(1)
	topicDetail.ReplicationFactor = int16(1)
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
		tracer.Errorf(glob.ProjName, "Error creating topic: %v", err.Error())
		return
	}
	tracer.Tracef(glob.ProjName, "Topic [%v] created", topic)

	// close connection to broker
	// broker.Close()
	return nil
}
