package dispatcher

import "context"

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
