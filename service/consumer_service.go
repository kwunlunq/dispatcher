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

func (c *consumerService) Subscribe(topic string, callback model.ConsumerCallback, opts ...model.Option) (errSignal <-chan error) {
	tmpErrSignal := make(chan error, 1)

	if !core.IsInitialized() {
		tmpErrSignal <- model.ErrNotInitialized
		close(tmpErrSignal)
		errSignal = tmpErrSignal
		return
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

	errSignal = c.subscribe(topic, consumerGroupID, callback, asyncNum, offsetOldest)
	return
}

func (c *consumerService) subscribe(topic string, groupID string, callback model.ConsumerCallback, asyncNum int, offsetOldest bool) (errSignal chan error) {
	errSignal = make(chan error, 1)

	if c.isTopicExisted(topic) {
		close(errSignal)
		return
	}
	c.addSubTopics(topic)

	var consumer sarama.ConsumerGroup
	var err error
	consumer, err = c.newConsumer(topic, offsetOldest, groupID)
	if err != nil {
		errSignal <- err
		close(errSignal)
		return
	}

	// Iterate over consumer sessions.
	core.Logger.Infof("Listening on topic [%v] with groupID [%v] by [%v] workers ...", topic, groupID, asyncNum)
	ctx := context.Background()
	topics := []string{topic}
	handler := consumerHandler{WorkerPoolService.MakeWorkerPool(callback, asyncNum, true)}

	go func() {
		consumer.Consume(ctx, topics, handler)
	}()

	go func() {
		var err error
		var errs []error

		err, _ = <-consumer.Errors() // blocked
		if err != nil {
			errs = append(errs, err)
			core.Logger.Errorf("Consumer consumer err: %v", err.Error())
		}

		err = consumer.Close()
		if err != nil {
			errs = append(errs, err)
			core.Logger.Errorf("Error closing consumer: %v", err.Error())
		}

		if len(errs) == 0 {
			close(errSignal)
			return
		}

		if len(errs) == 1 {
			errSignal <- errs[0]
			close(errSignal)
			return
		}

		var errMsgs []string
		for _, err := range errs {
			if err != nil {
				errMsgs = append(errMsgs, err.Error())
			}
		}

		err = fmt.Errorf(strings.Join(errMsgs, "; "))
		errSignal <- err
		close(errSignal)

	}()

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
