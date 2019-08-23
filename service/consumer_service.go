package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"gitlab.paradise-soft.com.tw/glob/dispatcher/glob/core"

	"github.com/Shopify/sarama"
	"gitlab.paradise-soft.com.tw/glob/dispatcher/model"
)

var ConsumerService = &consumerService{[]string{}}

type consumerService struct {
	subscribedTopics []string
}

func (c *consumerService) Subscribe(topic string, callback model.ConsumerCallback, opts ...model.Option) error {
	if !core.IsInitialized() {
		return model.ErrNotInitialized
	}

	dis := model.MakeDispatcher(opts)
	asyncNum := 1
	if dis.ConsumerAsyncNum > 1 {
		asyncNum = dis.ConsumerAsyncNum
	}

	offsetOldest := true
	if dis.ConsumerOmitOldMsg {
		offsetOldest = false
	}

	consumerGroupID := core.Config.DefaultGroupID
	if strings.TrimSpace(dis.ConsumerGroupID) != "" {
		consumerGroupID = dis.ConsumerGroupID
	}

	return c.subscribe(topic, consumerGroupID, callback, asyncNum, offsetOldest)
}

func (c *consumerService) subscribe(topic string, groupID string, callback model.ConsumerCallback, asyncNum int, offsetOldest bool) (err error) {

	if c.isTopicExisted(topic) {
		return
	}
	c.addSubTopics(topic)

	var consumer sarama.ConsumerGroup
	consumer, err = c.newConsumer(topic, offsetOldest, groupID)
	if err != nil {
		return
	}

	// Iterate over consumer sessions.
	core.Logger.Infof("Listening on topic [%v] with groupID [%v] by [%v] workers ...", topic, groupID, asyncNum)
	ctx := context.Background()
	topics := []string{topic}
	handler := consumerHandler{WorkerPoolService.MakeWorkerPool(callback, asyncNum, true)}
	go consumer.Consume(ctx, topics, handler)

	err = <-consumer.Errors()
	core.Logger.Errorf("Consumer consumer err: %v", err.Error())
	closeErr := consumer.Close()
	if closeErr != nil {
		core.Logger.Errorf("Error closing consumer: %v", closeErr.Error())
		err = fmt.Errorf("%v; %v", err.Error(), closeErr.Error())
	}

	return
}

func (c *consumerService) newConsumer(topic string, offsetOldest bool, groupID string) (group sarama.ConsumerGroup, err error) {
	err = TopicService.Create(topic)
	if err != nil {
		return
	}
	time.Sleep(100 * time.Millisecond)

	saramaConfig := core.SaramaConfig
	if offsetOldest {
		saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	} else {
		saramaConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	}

	group, err = sarama.NewConsumerGroup(core.Config.Brokers, groupID, &saramaConfig)
	if err != nil {
		core.Logger.Errorf("Error creating consumer group: %v", err.Error())
		return
	}
	return
}

func (c *consumerService) addSubTopics(topic string) {
	c.subscribedTopics = append(c.subscribedTopics, topic)
}

func (c *consumerService) isTopicExisted(t string) bool {
	for _, topic := range c.subscribedTopics {
		if topic == t {
			return true
		}
	}
	return false
}

type consumerHandler struct {
	pool WorkerPool
}

func (consumerHandler) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (consumerHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (h consumerHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	core.Logger.Debugf("Consumer claim [init/high offset: %v/%v, topic: %v, partition: %v]", claim.InitialOffset(), claim.HighWaterMarkOffset(), claim.Topic(), claim.Partition())

	// Receive processed messages
	go h.markMessage(sess)

	// Process messages
	h.claimMessage(claim)

	core.Logger.Debugf("Finished consuming claim.")
	return nil
}

func (h consumerHandler) claimMessage(claim sarama.ConsumerGroupClaim) {
	for msg := range claim.Messages() {
		h.pool.AddJob(msg)
	}
}

func (h consumerHandler) markMessage(sess sarama.ConsumerGroupSession) {
	for result := range h.pool.Results() {
		sess.MarkMessage(result, "")
	}
}
