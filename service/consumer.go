package service

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"gitlab.paradise-soft.com.tw/glob/dispatcher/glob/core"
	"gitlab.paradise-soft.com.tw/glob/dispatcher/model"
	"sync"
)

type Consumer struct {
	Topic            string
	GroupID          string
	ConsumeErrChan   chan error
	CancelConsume    context.CancelFunc
	CancelWorkerPool context.CancelFunc

	ctx            context.Context
	closeOnce      sync.Once
	handler        consumerHandlerSarama
	saramaConsumer sarama.ConsumerGroup
	saramaCtx      context.Context
	saramaCancel   context.CancelFunc
	closed         chan struct{}
}

func newConsumer(group sarama.ConsumerGroup, topic, groupID string, asyncNum int, callback model.MessageConsumerCallback, isMarkOffsetOnError bool) *Consumer {

	ctx, cancel := context.WithCancel(context.Background())

	wpCtx, wpCancel := context.WithCancel(context.Background())

	saramaCtx, saramaCancel := context.WithCancel(context.Background())

	started := make(chan struct{}, 1)
	handler := consumerHandlerSarama{
		pool:        WorkerPoolService.MakeWorkerPool(wpCtx, asyncNum, callback, groupID, isMarkOffsetOnError),
		startedChan: started,
	}

	consumeErrChan := make(chan error, 5)

	return &Consumer{
		Topic:            topic,
		GroupID:          groupID,
		ConsumeErrChan:   consumeErrChan,
		handler:          handler,
		ctx:              ctx,
		CancelConsume:    cancel,
		CancelWorkerPool: wpCancel,
		saramaConsumer:   group,
		saramaCancel:     saramaCancel,
		saramaCtx:        saramaCtx,
		closed:           make(chan struct{}),
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

		// Notify consumer been closed
		close(c.closed)

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
		core.Logger.Debugf("Error closing consumer of topic [%v], groupID [%v], err [%v]", c.Topic, c.GroupID, err)
	}
}
