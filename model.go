package dispatcher

import (
	"context"
	"gitlab.paradise-soft.com.tw/glob/dispatcher/glob/core"
)

type KafkaConfig core.KafkaConfig

type SubscriberCtrl struct {
	cancelFunc context.CancelFunc
	errors     <-chan error
}

func (c *SubscriberCtrl) Stop() {
	c.cancelFunc()
}

func (c *SubscriberCtrl) Errors() <-chan error {
	return c.errors
}
