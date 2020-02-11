package service

import (
	"github.com/pkg/errors"
	"gitlab.paradise-soft.com.tw/glob/dispatcher/glob"
	"sync"
	"time"

	"gitlab.paradise-soft.com.tw/glob/dispatcher/glob/core"

	"github.com/Shopify/sarama"
	"gitlab.paradise-soft.com.tw/glob/dispatcher/model"
)

var ConsumerService = &consumerService{}

type consumerService struct {
	lockers       sync.Map
	subscriptions sync.Map
}

func (s *consumerService) Subscribe(topic string, callback model.BytesConsumerCallback, opts ...model.Option) (consumer *Consumer, err error) {
	consumer, err = s.SubscribeMessage(topic, callback.Wrap(), opts...)
	return
}

func (s *consumerService) SubscribeMessage(topic string, callback model.MessageConsumerCallback, opts ...model.Option) (consumer *Consumer, err error) {
	if !core.IsInitialized() {
		err = model.ErrNotInitialized
		return
	}
	dis := model.MakeDispatcher(opts)

	// Subscribe
	consumer, err = s.subscribe(topic, dis.ConsumerGroupID, callback, dis.ConsumerAsyncNum, !dis.ConsumerOmitOldMsg, dis.ConsumerIsCommitOffsetOnError)
	return
}

func (s *consumerService) SubscribeWithRetry(topic string, callback model.BytesConsumerCallback, failRetryLimit int, getRetryDuration func(failCount int) time.Duration, opts ...model.Option) (ctrl *ConsumerWithRetryCtrl) {
	consumerWithRetry := NewConsumerWithRetry(topic, failRetryLimit, getRetryDuration)
	consumerWithRetry.Do(callback.Wrap(), opts...)
	ctrl = &consumerWithRetry.controller
	return
}

func (s *consumerService) SubscribeWithRetryMessage(topic string, callback model.MessageConsumerCallback, failRetryLimit int, getRetryDuration func(failCount int) time.Duration, opts ...model.Option) (ctrl *ConsumerWithRetryCtrl) {
	consumerWithRetry := NewConsumerWithRetry(topic, failRetryLimit, getRetryDuration)
	consumerWithRetry.Do(callback, opts...)
	ctrl = &consumerWithRetry.controller
	return
}

func (s *consumerService) subscribe(topic string, groupID string, callback model.MessageConsumerCallback, asyncNum int, offsetOldest bool, isMarkOffsetOnError bool) (consumer *Consumer, err error) {

	// Create consumer
	consumer, err = s.newConsumer(topic, offsetOldest, groupID, callback, asyncNum, isMarkOffsetOnError)
	if err != nil {
		err = errors.Wrapf(err, "error creating Consumer of Topic: [%v], groupID: [%v]", topic, groupID)
		return
	}

	// Consume
	go func() {
		var consumeErr error
		select {
		case consumeErr = <-consumer.consume():
			consumer.saramaCancel()
		case consumeErr = <-consumer.saramaConsumer.Errors():
			consumer.saramaCancel()
		case <-consumer.ctx.Done():
			consumeErr = model.ErrConsumerStoppedManually
		}
		core.Logger.Debug("Consumer stopped due to err: ", consumeErr)
		consumer.close(consumeErr)
	}()

	// Block until consumption start
	<-consumer.handler.startedChan

	core.Logger.Infof("Listening on Topic [%v] with groupID [%v] by [%v] workers ...", topic, consumer.GroupID, asyncNum)
	return
}

func (c *Consumer) consume() (errChan chan error) {
	errChan = make(chan error)
	var isCancelled bool
	go func() {
		<-c.ctx.Done()
		isCancelled = true
		core.Logger.Debugf("TEST - consume() received c.ctx.Done() signal")
	}()
	go func() {
		for {
			core.Logger.Debugf("TEST - start consuming topic [%v], groupID [%v]", c.Topic, c.GroupID)
			err := c.saramaConsumer.Consume(c.saramaCtx, []string{c.Topic}, &c.handler)
			core.Logger.Debugf("TEST - consumer leaving: [%v], topic [%v], groupID [%v]", err, c.Topic, c.GroupID)
			if err != nil {
				err = errors.Wrapf(err, "error stop consume establishing Consumer of Topic [%v]", c.Topic)
				errChan <- err
				close(errChan)
				return
			}
			if isCancelled {
				core.Logger.Info("Consume cancelled due to manually stopped")
				return
			}
			core.Logger.Debugf("Rebalancing consumer group of topic: [%v], groupID: [%v]", c.Topic, c.GroupID)
		}
	}()
	return
}

func (s *consumerService) newConsumer(topic string, offsetOldest bool, groupID string, callback model.MessageConsumerCallback, asyncNum int, isMarkOffsetOnError bool) (dispatcherConsumer *Consumer, err error) {

	// Create consumer group for each topic
	formattedGroupID := glob.FormatGroupID(topic, groupID)

	// Create topic
	err = s.createTopic(topic, formattedGroupID)
	if err != nil {
		return
	}

	// Create sarama consumer
	var group sarama.ConsumerGroup
	group, err = s.newSaramaConsumer(topic, offsetOldest, formattedGroupID)
	if err != nil {
		err = errors.Wrapf(err, "error creating sarama Consumer of Topic [%v]", topic)
		s.removeSubscribingTopic(topic, formattedGroupID)
		return
	}

	// Wrap into dispatcher consumer
	dispatcherConsumer = newConsumer(group, topic, formattedGroupID, asyncNum, callback, isMarkOffsetOnError)
	return
}

func (s *consumerService) newSaramaConsumer(topic string, offsetOldest bool, groupID string) (group sarama.ConsumerGroup, err error) {
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

// createTopic 檢查topic已訂閱, 創建topic
func (s *consumerService) createTopic(topic, groupID string) (err error) {
	lockerI, _ := s.lockers.LoadOrStore(topic, new(sync.Mutex))
	locker := lockerI.(*sync.Mutex)
	locker.Lock()
	defer locker.Unlock()

	if s.isTopicScribing(topic, groupID) {
		err = model.ErrSubscribeOnSubscribedTopic
		return
	}

	err = TopicService.Create(topic)
	if err != nil {
		return
	}
	s.addToSubscribingTopics(topic, groupID)
	return
}

func (s *consumerService) isTopicScribing(topic, groupID string) (existed bool) {
	_, existed = s.subscriptions.Load(topic + groupID)
	return
}

func (s *consumerService) addToSubscribingTopics(topic, groupID string) {
	s.subscriptions.Store(topic+groupID, struct{}{})
}

func (s *consumerService) removeSubscribingTopic(topic, groupID string) {
	s.subscriptions.Delete(topic + groupID)
	TopicService.RemoveMapEntry(topic)
}
