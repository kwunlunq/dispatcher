package service

import (
	"context"
	"github.com/pkg/errors"
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

func (consumerService *consumerService) Subscribe(topic string, callback model.ConsumerCallback, opts ...model.Option) (consumeErrChan chan error, cancelFunc context.CancelFunc, err error) {
	if !core.IsInitialized() {
		err = model.ErrNotInitialized
		return
	}

	dis := model.MakeDispatcher(opts)
	consumerGroupID := core.Config.DefaultGroupID
	if strings.TrimSpace(dis.ConsumerGroupID) != "" {
		consumerGroupID = dis.ConsumerGroupID
	}

	consumeErrChan, cancelFunc, err = consumerService.subscribe(topic, consumerGroupID, callback, dis.ConsumerAsyncNum, !dis.ConsumerOmitOldMsg)
	return
}

func (consumerService *consumerService) subscribe(topic string, groupID string, callback model.ConsumerCallback, asyncNum int, offsetOldest bool) (consumeErrChan chan error, cancelFunc context.CancelFunc, err error) {
	if consumerService.isTopicExisted(topic) {
		err = model.ErrSubscribeExistedTopic
		return
	}
	consumerService.addSubTopic(topic)

	var consumer sarama.ConsumerGroup
	consumer, err = consumerService.new(topic, offsetOldest, groupID)
	if err != nil {
		consumerService.removeSubTopic(topic)
		return
	}

	// Create ctx to close consumer if needed
	ctx := context.Background()
	ctx, cancelFunc = context.WithCancel(ctx)

	// Iterate over consumer sessions.
	topics := []string{topic}
	started := make(chan struct{}, 1)
	handler := consumerHandler{
		pool:        WorkerPoolService.MakeWorkerPool(callback, asyncNum, true, ctx),
		startedChan: started,
	}

	consumeErrChan = make(chan error, 2)

	// Consume message
	go func() {
		consumeErr := consumer.Consume(ctx, topics, &handler) // blocked
		if consumeErr == nil {
			consumeErr = model.ErrConsumeStopWithoutError
		}
		consumeErr = errors.Wrapf(consumeErr, "error establishing consumer of topic [%v]", topic)
		consumerService.close(topic, consumer, consumeErrChan, consumeErr, &handler, cancelFunc)
	}()

	go func() {
		select {
		// Error occurs on consuming
		case consumeErr := <-consumer.Errors():
			if consumeErr == nil {
				consumeErr = model.ErrConsumeStopWithoutError
			}
			consumeErr = errors.Wrapf(consumeErr, "error listening on topic [%v]", topic)
			consumerService.close(topic, consumer, consumeErrChan, consumeErr, &handler, cancelFunc)
		// Stopped by user
		case <-ctx.Done():
			consumerService.close(topic, consumer, consumeErrChan, nil, &handler, cancelFunc)
		}
	}()

	<-started
	core.Logger.Infof("Listening on topic [%v] with groupID [%v] by [%v] workers ...", topic, groupID, asyncNum)
	return
}

func (consumerService *consumerService) new(topic string, offsetOldest bool, groupID string) (group sarama.ConsumerGroup, err error) {
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

func (consumerService *consumerService) close(topic string, consumer sarama.ConsumerGroup, consumeErrChan chan error, err error, handler *consumerHandler, cancelFunc context.CancelFunc) {
	if err != nil {
		core.Logger.Error(err.Error())
		consumeErrChan <- err
	} else {
		core.Logger.Infof("Consumer on topic [%v] was closed manually.", topic)
	}
	cancelFunc() // stop workers
	close(consumeErrChan)
	if consumer != nil {
		err := consumer.Close()
		if err != nil {
			err = errors.Wrap(err, "error closing consumer")
			core.Logger.Error(err.Error())
		}
	}
	consumerService.removeSubTopic(topic)
	// Close started chan manually in case of error occurs on establishing consumer and the subscribe() func had returned.
	handler.started()
}

func (consumerService *consumerService) addSubTopic(topic string) {
	consumerService.subscribedTopics.Store(topic, struct{}{})
}

func (consumerService *consumerService) isTopicExisted(topic string) (existed bool) {
	_, existed = consumerService.subscribedTopics.Load(topic)
	return
}

func (consumerService *consumerService) removeSubTopic(topic string) {
	consumerService.subscribedTopics.Delete(topic)
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
		var consumeErrChan chan error
		consumeErrChan, _, err = ConsumerService.Subscribe(topic, callback, opts...)

		// Handle subscriber creation error
		if err != nil {
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
		consumeErr, _ := <-consumeErrChan
		if consumeErr != nil {
			failCount++
			core.Logger.Error("Consuming err:", consumeErr.Error(), "counting:", failCount)
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
