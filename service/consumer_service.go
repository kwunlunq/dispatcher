package service

import (
	"context"
	"github.com/pkg/errors"
	"gitlab.paradise-soft.com.tw/glob/dispatcher/glob"
	"strings"
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

func (c *consumerService) Subscribe(topic string, callback model.BytesConsumerCallback, opts ...model.Option) (consumer *Consumer, err error) {
	consumer, err = c.SubscribeWithMessageCallback(topic, callback.Wrap(), opts...)
	return
}

func (c *consumerService) SubscribeWithMessageCallback(topic string, callback model.MessageConsumerCallback, opts ...model.Option) (consumer *Consumer, err error) {
	if !core.IsInitialized() {
		err = model.ErrNotInitialized
		return
	}
	dis := model.MakeDispatcher(opts)

	// Subscribe
	consumer, err = c.subscribe(topic, dis.ConsumerGroupID, callback, dis.ConsumerAsyncNum, !dis.ConsumerOmitOldMsg, dis.ConsumerLagCountHandler, dis.ConsumerLagCountInterval)
	return
}

func (c *consumerService) subscribe(topic string, groupID string, callback model.MessageConsumerCallback, asyncNum int, offsetOldest bool, lagCountHandler func(lagCount int), lagCountInterval time.Duration) (consumer *Consumer, err error) {

	// Create consumer
	consumer, err = c.newConsumer(topic, offsetOldest, groupID, callback, asyncNum, lagCountHandler, lagCountInterval)
	if err != nil {
		err = errors.Wrapf(err, "error creating Consumer of Topic: [%v], groupID: [%v]", topic, groupID)
		return
	}

	// Monitor lag count
	if consumer.LagCountHandler != nil {
		go consumer.monitorLagCount()
	}

	// Consume
	go func() {
		var consumeErr error
		select {
		case consumeErr = <-consumer.consume():
			consumer.CancelConsume()
		case consumeErr = <-consumer.saramaConsumer.Errors():
			consumer.CancelConsume()
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
			err := c.saramaConsumer.Consume(c.ctx, []string{c.Topic}, &c.handler)
			core.Logger.Debugf("TEST - consumer leaving: [%v], topic [%v], groupID [%v]", err, c.Topic, c.GroupID)
			if err != nil {
				err = errors.Wrapf(err, "error stop consume establishing Consumer of Topic [%v]", c.Topic)
				errChan <- err
				close(errChan)
				return
			}
			if isCancelled {
				core.Logger.Debug("Consume cancelled due to manually stopped")
				return
			}
			core.Logger.Debugf("Rebalancing consumer group of topic: [%v], groupID: [%v]", c.Topic, c.GroupID)
		}
	}()
	return
}

func (c *consumerService) newConsumer(topic string, offsetOldest bool, groupID string, callback model.MessageConsumerCallback, asyncNum int, lagCountHandler func(lagCount int), lagCountInterval time.Duration) (dispatcherConsumer *Consumer, err error) {

	// Create topic
	err = c.createTopic(topic)
	if err != nil {
		return
	}

	// Create consumer group for each topic
	groupID = c.getValidGroupID(topic, groupID)

	// Create sarama consumer
	var group sarama.ConsumerGroup
	group, err = c.newSaramaConsumer(topic, offsetOldest, groupID)
	if err != nil {
		err = errors.Wrapf(err, "error creating sarama Consumer of Topic [%v]", topic)
		c.removeSubscribingTopic(topic)
		return
	}

	// Wrap into dispatcher consumer
	dispatcherConsumer = newConsumer(group, topic, groupID, asyncNum, callback, lagCountHandler, lagCountInterval)
	return
}

func (c *consumerService) newSaramaConsumer(topic string, offsetOldest bool, groupID string) (group sarama.ConsumerGroup, err error) {
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
func (c *consumerService) createTopic(topic string) (err error) {
	lockerI, _ := c.lockers.LoadOrStore(topic, new(sync.Mutex))
	locker := lockerI.(*sync.Mutex)
	locker.Lock()
	defer locker.Unlock()

	if c.isTopicScribing(topic) {
		err = model.ErrSubscribeOnSubscribedTopic
		return
	}

	err = TopicService.Create(topic)
	if err != nil {
		return
	}
	c.addToSubscribingTopics(topic)
	return
}

func (c *consumerService) isTopicScribing(topic string) (existed bool) {
	_, existed = c.subscriptions.Load(topic)
	return
}

func (c *consumerService) addToSubscribingTopics(topic string) {
	c.subscriptions.Store(topic, struct{}{})
}

func (c *consumerService) removeSubscribingTopic(topic string) {
	c.subscriptions.Delete(topic)
	TopicService.RemoveMapEntry(topic)
}

func (c *consumerService) getValidGroupID(topic, groupID string) string {
	if strings.TrimSpace(groupID) == "" {
		groupID = core.Config.DefaultGroupID
	}
	groupID = glob.AppendSuffix(groupID, topic, ":")
	return groupID
}

type Consumer struct {
	Topic            string
	GroupID          string
	ConsumeErrChan   chan error
	CancelConsume    context.CancelFunc
	CancelWorkerPool context.CancelFunc
	LagCountHandler  func(lagCount int)
	LagCountInterval time.Duration

	ctx                      context.Context
	closeOnce                sync.Once
	handler                  consumerHandler
	saramaConsumer           sarama.ConsumerGroup
	saramaConsumerCancelFunc context.CancelFunc
}

func newConsumer(group sarama.ConsumerGroup, topic, groupID string, asyncNum int, callback model.MessageConsumerCallback, lagCountHandler func(lagCount int), lagCountInterval time.Duration) *Consumer {

	ctx, cancel := context.WithCancel(context.Background())

	wpCtx, wpCancel := context.WithCancel(context.Background())

	started := make(chan struct{}, 1)
	handler := consumerHandler{
		pool:        WorkerPoolService.MakeWorkerPool(wpCtx, asyncNum, callback, groupID),
		startedChan: started,
	}

	consumeErrChan := make(chan error, 5)

	return &Consumer{
		Topic:            topic,
		GroupID:          groupID,
		ConsumeErrChan:   consumeErrChan,
		handler:          handler,
		CancelConsume:    cancel,
		CancelWorkerPool: wpCancel,
		ctx:              ctx,
		saramaConsumer:   group,
		LagCountHandler:  lagCountHandler,
		LagCountInterval: lagCountInterval,
	}
}

// close 關閉subscriber, 清除相關資源
func (c *Consumer) close(err error) {
	core.Logger.Debugf("Closing consumer [%v] of topic [%v] due to error: %v", c.GroupID, c.Topic, err)
	c.closeOnce.Do(func() {

		err = errors.Wrap(err, "err consumer closed")

		core.Logger.Debugf("(Once) Closing consumer [%v] of topic [%v] due to error: %v", c.GroupID, c.Topic, err)

		// Close sarama consumer
		c.closeSaramaConsumer()

		// Stop workers (should be done after sarama's consumer closed and all messages are consumed, or deadlock may happened)
		c.CancelWorkerPool()

		// Close started chan manually in case of error occurs on establishing consumer and the subscribe() func had returned.
		c.handler.started()

		// Remove from subscribing topics
		ConsumerService.removeSubscribingTopic(c.Topic)

		// Send error to user
		if err != nil {
			c.ConsumeErrChan <- err
		}
		close(c.ConsumeErrChan)

		core.Logger.Debugf("Consumer closed: [%v], topic [%v], groupID [%v]", err, c.Topic, c.GroupID)
	})
	return
}

func (c *Consumer) closeSaramaConsumer() {
	if c.saramaConsumer == nil {
		return
	}
	err := c.saramaConsumer.Close()
	if err != nil {
		core.Logger.Errorf("Error closing consumer of topic [%v], groupID [%v], err [%v]", c.Topic, c.GroupID, err)
	}
}

func (c *Consumer) monitorLagCount() {
	ticker := time.NewTicker(c.LagCountInterval)
	for {
		select {
		case <-ticker.C:
			var lagCount int
			// Get lag count from API
			c.LagCountHandler(lagCount)
			core.Logger.Infof("Lag count: %v, topic: %v, consumerID: %v\n", lagCount, c.Topic, c.GroupID)
		case <-c.ctx.Done():
			return
		}
	}
}

type consumerHandler struct {
	pool        WorkerPool
	startedChan chan struct{}
	startedOnce sync.Once
}

func (h *consumerHandler) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (h *consumerHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim 處理收到的訊息, 執行次數 = 分到的partition數量
func (h *consumerHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	h.started()
	go h.markProcessedMessages(sess)
	h.processMessages(claim) // blocking
	return nil
}

func (h *consumerHandler) processMessages(claim sarama.ConsumerGroupClaim) {
	core.Logger.Debugf("Claiming message partition [%v]", claim.Partition())
	for msg := range claim.Messages() {
		h.pool.AddJob(msg)
	}
	core.Logger.Debugf("Claimed message partition [%v]", claim.Partition())
}

func (h *consumerHandler) markProcessedMessages(sess sarama.ConsumerGroupSession) {
	for {
		select {
		case result, ok := <-h.pool.Processed():
			if !ok {
				return
			}
			core.Logger.Debugf("Message marked: %v: %v", result.Partition, result.Offset)
			sess.MarkMessage(result, "")
		case <-h.pool.Context().Done():
			return
		}
	}
}

func (h *consumerHandler) started() {
	h.startedOnce.Do(func() {
		h.startedChan <- struct{}{}
	})
}

func (c *consumerService) SubscribeWithRetry(topic string, callback model.BytesConsumerCallback, failRetryLimit int, getRetryDuration func(failCount int) time.Duration, opts ...model.Option) (err error) {
	return c.subscribeWithRetryMessageCallback(topic, callback.Wrap(), failRetryLimit, getRetryDuration, opts...)
}

func (c *consumerService) subscribeWithRetryMessageCallback(topic string, callback model.MessageConsumerCallback, failRetryLimit int, getRetryDuration func(failCount int) time.Duration, opts ...model.Option) (err error) {
	failCount := 0

	// Blocking until error count reach limit
	for {
		// Create subscriber
		var c *Consumer
		c, err = ConsumerService.SubscribeWithMessageCallback(topic, callback, opts...)

		// Handle subscriber creation error
		if err != nil {
			// Stop create subscriber if the topic had been subscribed
			if errors.Cause(err) == model.ErrSubscribeOnSubscribedTopic {
				core.Logger.Debugf("Stop create consumer, since the topic [%v] has been subscribed", topic)
				break
			}
			failCount++
			core.Logger.Error("Create consumer err:", err.Error(), ", counting:", failCount)
			if failCount >= failRetryLimit {
				core.Logger.Error("Error count reach limit, leaving now")
				break
			}
			time.Sleep(getRetryDuration(failCount))
			continue
		}

		// Subscriber created successfully, reset failCount
		failCount = 0

		// Handle error during subscription
		consumeErr, _ := <-c.ConsumeErrChan
		if consumeErr != nil {
			failCount++
			core.Logger.Errorf("Error during consumption: [%v], counting [%v], topic [%v], groupID [%v]", consumeErr.Error(), failCount, c.Topic, c.GroupID)
			if failCount >= failRetryLimit {
				core.Logger.Errorf("Error count reach limit, closing now, topic [%v], groupID [%v]", c.Topic, c.GroupID)
				break
			}
			time.Sleep(getRetryDuration(failCount))
			continue
		}

		// Subscriber been stopped manually
		core.Logger.Error("Consumer terminated without error")
		time.Sleep(getRetryDuration(failCount))
	}

	return
}
