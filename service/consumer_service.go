package service

import (
	"context"
	"github.com/pkg/errors"
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

func (c *consumerService) Subscribe(topic string, callback model.ConsumerCallback, opts ...model.Option) (err error) {
	if !core.IsInitialized() {
		err = model.ErrNotInitialized
		return
	}

	dis := model.MakeDispatcher(opts)
	consumerGroupID := core.Config.DefaultGroupID
	if strings.TrimSpace(dis.ConsumerGroupID) != "" {
		consumerGroupID = dis.ConsumerGroupID
	}

	err = c.subscribe(topic, consumerGroupID, callback, dis.ConsumerAsyncNum, !dis.ConsumerOmitOldMsg)
	return
}

func (c *consumerService) subscribe(topic string, groupID string, callback model.ConsumerCallback, asyncNum int, offsetOldest bool) (err error) {
	if c.isTopicExisted(topic) {
		err = model.ErrSubscribeExistedTopic
		return
	}
	c.addSubTopics(topic)

	var consumer sarama.ConsumerGroup
	consumer, err = c.newConsumer(topic, offsetOldest, groupID)
	if err != nil {
		return
	}

	// Close consumer group on panic
	defer func() {
		err = consumer.Close()
		if err != nil {
			core.Logger.Errorf("Error closing consumer: %v", err.Error())
		}
	}()

	// Iterate over consumer sessions.
	core.Logger.Infof("Listening on topic [%v] with groupID [%v] by [%v] workers ...", topic, groupID, asyncNum)
	ctx := context.Background()
	topics := []string{topic}
	handler := consumerHandler{WorkerPoolService.MakeWorkerPool(callback, asyncNum, true)}

	errChan := make(chan error, 2)

	// Consume message
	go func() {
		err = consumer.Consume(ctx, topics, handler) // blocked
		if err != nil {
			errChan <- err
		}
	}()

	// Listen on error
	go func() {
		errChan <- <-consumer.Errors()
	}()

	// Process error
	err, _ = <-errChan
	closeErr := consumer.Close()
	if closeErr != nil {
		err = errors.Wrap(err, closeErr.Error())
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
