package service

import (
	"context"
	"github.com/pkg/errors"
	"gitlab.paradise-soft.com.tw/glob/dispatcher/glob/core"
	"gitlab.paradise-soft.com.tw/glob/dispatcher/model"
	"time"
)

type consumerWithRetry struct {
	topic              string
	controller         ConsumerWithRetryCtrl
	failCount          int
	failRetryLimit     int
	consumer           Consumer
	getRetryDuration   func(failCount int) time.Duration
	setConsumeStarted  func()
	waitConsumeStarted func()
}

type ConsumerWithRetryCtrl struct {
	ConsumeErrorChan chan error
	CancelConsume    context.CancelFunc
	GroupID          string

	consumeCtx context.Context
}

func NewConsumerWithRetry(topic string, failRetryLimit int, getRetryDuration func(failCount int) time.Duration) (cr *consumerWithRetry) {
	consumeErrChan := make(chan error, 1)
	ctx := context.Background()
	retryConsumeCtx, cancel := context.WithCancel(ctx)
	consumerCtrl := ConsumerWithRetryCtrl{
		ConsumeErrorChan: consumeErrChan,
		CancelConsume:    cancel,
		consumeCtx:       retryConsumeCtx,
	}
	cr = &consumerWithRetry{
		topic:            topic,
		controller:       consumerCtrl,
		failRetryLimit:   failRetryLimit,
		getRetryDuration: getRetryDuration,
	}
	cr.genStartedMethod()
	return
}

func (cr *consumerWithRetry) Do(callback model.MessageConsumerCallback, opts ...model.Option) {
	go cr.do(callback, opts...)
	cr.waitConsumeStarted()
	return
}

func (cr *consumerWithRetry) do(callback model.MessageConsumerCallback, opts ...model.Option) {
	for {
		// Create consumer & start consuming (non-blocking)
		consumer, creationErr := ConsumerService.SubscribeMessage(cr.topic, callback, opts...)
		if consumer != nil {
			cr.controller.GroupID = consumer.GroupID
		}
		cr.setConsumeStarted()
		if creationErr != nil {
			if !cr.handleCreationError(creationErr) {
				return
			}
			continue
		}

		// Consumer created
		cr.consumer = *consumer
		cr.failCount = 0

		// Listen consume error or cancel signal by user
		select {
		case <-cr.controller.consumeCtx.Done():
			cr.canceledByUser()
			return
		case consumeErr, _ := <-cr.consumer.ConsumeErrChan:
			if !cr.handleConsumeError(consumeErr) {
				return
			}
		}
	}
}

func (cr *consumerWithRetry) handleCreationError(createErr error) (isContinueRetry bool) {

	if errors.Cause(createErr) == model.ErrSubscribeOnSubscribedTopic {
		core.Logger.Debugf("Stop create consumer, topic [%v] is already subscribing", cr.consumer.Topic)
		cr.controller.ConsumeErrorChan <- createErr
		close(cr.controller.ConsumeErrorChan)
		return
	}

	cr.failCount++
	core.Logger.Error("Create consumer err:", createErr.Error(), ", counting:", cr.failCount)
	if cr.reachErrorLimit(createErr) {
		return
	}

	isContinueRetry = true
	cr.sleep()
	return
}

func (cr *consumerWithRetry) reachErrorLimit(err error) (isReachLimit bool) {
	if cr.failCount >= cr.failRetryLimit {
		core.Logger.Error("Error count reach limit, leaving now")
		cr.controller.ConsumeErrorChan <- err
		close(cr.controller.ConsumeErrorChan)
		isReachLimit = true
	}
	return
}

func (cr *consumerWithRetry) handleConsumeError(consumeErr error) (isContinueRetry bool) {
	if consumeErr != nil {
		cr.failCount++
		core.Logger.Errorf("Error during consumption: [%v], counting [%v], topic [%v], groupID [%v]", consumeErr.Error(), cr.failCount, cr.consumer.Topic, cr.consumer.GroupID)
		if cr.reachErrorLimit(consumeErr) {
			return
		}
	} else {
		core.Logger.Error("Consumer terminated without error")
	}
	isContinueRetry = true
	cr.sleep()
	return
}

func (cr *consumerWithRetry) canceledByUser() {
	cr.consumer.CancelConsume()
	close(cr.controller.ConsumeErrorChan)
	return
}

func (cr *consumerWithRetry) sleep() {
	time.Sleep(cr.getRetryDuration(cr.failCount))
}

func (cr *consumerWithRetry) genStartedMethod() {
	startedChan := make(chan struct{})
	cr.setConsumeStarted = func() {
		select {
		case startedChan <- struct{}{}:
			return
		default:
			return
		}
	}
	cr.waitConsumeStarted = func() {
		<-startedChan
	}
	return
}
