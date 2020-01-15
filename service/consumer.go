package service

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"gitlab.paradise-soft.com.tw/glob/dispatcher/glob/core"
	"gitlab.paradise-soft.com.tw/glob/dispatcher/model"
	"sync"
	"time"
)

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
	handler                  consumerHandlerSarama
	saramaConsumer           sarama.ConsumerGroup
	saramaConsumerCancelFunc context.CancelFunc
}

func newConsumer(group sarama.ConsumerGroup, topic, groupID string, asyncNum int, callback model.MessageConsumerCallback, lagCountHandler func(lagCount int), lagCountInterval time.Duration) *Consumer {

	ctx, cancel := context.WithCancel(context.Background())

	wpCtx, wpCancel := context.WithCancel(context.Background())

	started := make(chan struct{}, 1)
	handler := consumerHandlerSarama{
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
		ConsumerService.removeSubscribingTopic(c.Topic, c.GroupID)

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
