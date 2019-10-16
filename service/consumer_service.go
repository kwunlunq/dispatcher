package service

import (
	"context"
	"fmt"
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
	subscribedTopics sync.Map
}

func (consumerService *consumerService) Subscribe(topic string, callback model.ConsumerCallback, opts ...model.Option) (c *Consumer, err error) {
	if !core.IsInitialized() {
		err = model.ErrNotInitialized
		return
	}

	dis := model.MakeDispatcher(opts)
	c, err = consumerService.subscribe(topic, dis.ConsumerGroupID, callback, dis.ConsumerAsyncNum, !dis.ConsumerOmitOldMsg)
	return
}

func (consumerService *consumerService) subscribe(topic string, groupID string, callback model.ConsumerCallback, asyncNum int, offsetOldest bool) (consumer *Consumer, err error) {

	// Create topic
	err = consumerService.createTopic(topic)
	if err != nil {
		return
	}

	// Create Consumer
	ctx := context.Background()
	consumer, err = consumerService.getNew(topic, offsetOldest, groupID, callback, asyncNum, ctx)
	if err != nil {
		err = errors.Wrapf(err, "error creating Consumer of Topic: [%v], groupID: [%v]", topic, groupID)
		consumerService.removeSubTopic(topic)
		return
	}
	groupID = consumer.GroupID

	// Consume message
	var consumeErr error
	go func() {
		for {
			consumeErr = consumer.saramaConsumer.Consume(context.Background(), []string{consumer.Topic}, &consumer.handler) // blocked
			if consumeErr != nil {
				consumeErr = errors.Wrapf(consumeErr, "error establishing Consumer of Topic [%v]", topic)
				consumer.close(consumeErr)
				break
			}
			// Reconnect when rebalance occurs
			core.Logger.Debugf("Rebalancing consumer group of topic: [%v], groupID: [%v]", topic, groupID)
		}
	}()

	// Collect cancel / error signal
	go func() {
		select {
		// Error occurs on consuming
		case consumeErr = <-consumer.saramaConsumer.Errors():
			if consumeErr == nil {
				consumeErr = model.ErrConsumeStopWithoutError
			}
			consumeErr = errors.Wrapf(consumeErr, "error listening on Topic [%v]", topic)
			consumer.close(consumeErr)
		// Stopped by user
		case <-ctx.Done():
			consumer.close(consumeErr)
		}
	}()

	<-consumer.handler.startedChan
	core.Logger.Infof("Listening on Topic [%v] with groupID [%v] by [%v] workers ...", topic, groupID, asyncNum)
	return
}

func (consumerService *consumerService) getNew(topic string, offsetOldest bool, groupID string, callback model.ConsumerCallback, asyncNum int, ctx context.Context) (dispatcherConsumer *Consumer, err error) {
	// Create consumer group for each topic
	groupID = consumerService.getValidGroupID(topic, groupID)

	// Create sarama consumer
	var group sarama.ConsumerGroup
	group, err = consumerService.newSaramaConsumer(topic, offsetOldest, groupID)
	if err != nil {
		err = errors.Wrapf(err, "error creating sarama Consumer of Topic [%v]", topic)
		return
	}

	// Wrap into dispatcher consumer
	dispatcherConsumer = newConsumer(group, topic, groupID, ctx, asyncNum, callback)
	return
}

func (consumerService *consumerService) newSaramaConsumer(topic string, offsetOldest bool, groupID string) (group sarama.ConsumerGroup, err error) {
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
func (consumerService *consumerService) createTopic(topic string) (err error) {
	// Check topic already subscribed
	if consumerService.isTopicAlreadySubscribed(topic) {
		err = model.ErrSubscribeOnSubscribedTopic
		return
	}

	// Create topic
	err = TopicService.Create(topic)
	if err != nil {
		return
	}
	consumerService.addToSubscribingTopics(topic)
	return
}

func (consumerService *consumerService) addToSubscribingTopics(topic string) {
	consumerService.subscribedTopics.Store(topic, struct{}{})
}

func (consumerService *consumerService) isTopicAlreadySubscribed(topic string) (existed bool) {
	_, existed = consumerService.subscribedTopics.Load(topic)
	return
}

func (consumerService *consumerService) removeSubTopic(topic string) {
	consumerService.subscribedTopics.Delete(topic)
	TopicService.RemoveMapEntry(topic)
}

func (consumerService *consumerService) getValidGroupID(topic, groupID string) string {
	if strings.TrimSpace(groupID) == "" {
		groupID = core.Config.DefaultGroupID
	}
	groupID = glob.AppendSuffix(groupID, topic, ":")
	return groupID
}

type Consumer struct {
	Topic          string
	GroupID        string
	ConsumeErrChan chan error
	CancelFunc     context.CancelFunc

	closeOnce      sync.Once
	handler        consumerHandler
	saramaConsumer sarama.ConsumerGroup
}

func newConsumer(group sarama.ConsumerGroup, topic, groupID string, ctx context.Context, asyncNum int, callback model.ConsumerCallback) *Consumer {

	var cancelFunc context.CancelFunc
	ctx, cancelFunc = context.WithCancel(ctx)

	started := make(chan struct{}, 1)
	handler := consumerHandler{
		pool:        WorkerPoolService.MakeWorkerPool(callback, asyncNum, true, ctx),
		startedChan: started,
	}

	consumeErrChan := make(chan error, 5)

	return &Consumer{
		Topic:          topic,
		GroupID:        groupID,
		ConsumeErrChan: consumeErrChan,
		handler:        handler,
		CancelFunc:     cancelFunc,
		saramaConsumer: group,
	}
}

// close 關閉subscriber, 清除相關資源
func (c *Consumer) close(err error) {
	c.closeOnce.Do(func() {
		// Stop workers
		c.CancelFunc()

		// Stop consumer
		if c.saramaConsumer != nil {
			closeErr := c.saramaConsumer.Close()
			if closeErr != nil {
				closeErr = errors.Wrapf(closeErr, "error closing consumer of topic [%v]", c.Topic)
				if err != nil {
					err = errors.Wrap(closeErr, "error closing consumer")
				} else {
					err = closeErr
				}
			}
		}

		// Log error
		logMessage := fmt.Sprintf("Closing subscriber of topic [%v], groupID [%v]", c.Topic, c.GroupID)
		if err != nil {
			logMessage += ": " + err.Error()
			core.Logger.Error(logMessage)
			c.ConsumeErrChan <- err
		} else {
			core.Logger.Error(logMessage)
		}

		// Close err chan
		close(c.ConsumeErrChan)

		// Clear other data
		ConsumerService.removeSubTopic(c.Topic)

		// Close started chan manually in case of error occurs on establishing consumer and the subscribe() func had returned.
		c.handler.started()
	})
	return
}

type consumerHandler struct {
	pool        WorkerPool
	startedChan chan struct{}
	once        sync.Once
}

func (h *consumerHandler) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (h *consumerHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

// 執行次數 = 分到的partition數量
func (h *consumerHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	core.Logger.Debugf("Consumer claim [init/high offset: %v/%v, topic: %v, partition: %v]", claim.InitialOffset(), claim.HighWaterMarkOffset(), claim.Topic(), claim.Partition())
	h.started()

	// Receive processed messages
	go h.markMessage(sess)

	// Process messages
	h.claimMessage(claim)

	core.Logger.Debugf("Finished consuming claim.")
	return nil
}

func (h *consumerHandler) claimMessage(claim sarama.ConsumerGroupClaim) {
	for msg := range claim.Messages() {
		h.pool.AddJob(msg)
	}
}

func (h *consumerHandler) markMessage(sess sarama.ConsumerGroupSession) {
	for {
		select {
		case result, ok := <-h.pool.Results():
			if !ok {
				return
			}
			sess.MarkMessage(result, "")
		case <-h.pool.Context().Done():
			return
		}
	}
}

func (h *consumerHandler) started() {
	h.once.Do(func() {
		h.startedChan <- struct{}{}
	})
}

func (consumerService *consumerService) SubscribeWithRetry(topic string, callback model.ConsumerCallback, failRetryLimit int, getRetryDuration func(failCount int) time.Duration, opts ...model.Option) (err error) {
	failCount := 0

	// Blocked until error count reach limit
	for {
		// Create subscriber
		var c *Consumer
		c, err = ConsumerService.Subscribe(topic, callback, opts...)

		// Handle subscriber creation error
		if err != nil {
			// Stop create subscriber if the topic had been subscribed
			if errors.Cause(err) == model.ErrSubscribeOnSubscribedTopic {
				core.Logger.Infof("Stop create consumer, since the topic [%v] has been subscribed", topic)
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
			core.Logger.Error("Consuming err: ", consumeErr.Error(), ", counting:", failCount)
			if failCount >= failRetryLimit {
				core.Logger.Error("Error count reach limit, closing now")
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
