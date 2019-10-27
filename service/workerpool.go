package service

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"gitlab.paradise-soft.com.tw/glob/dispatcher/model"
	"runtime/debug"
	"time"

	"gitlab.paradise-soft.com.tw/glob/dispatcher/glob"
	"gitlab.paradise-soft.com.tw/glob/dispatcher/glob/core"

	"github.com/Shopify/sarama"
)

var (
	WorkerPoolService = workerPoolService{}
)

type workerPoolService struct{}

type WorkerPool interface {
	AddJob(job *sarama.ConsumerMessage)
	Processed() <-chan *sarama.ConsumerMessage
	Context() context.Context
}

type workerPool struct {
	jobs              chan *sarama.ConsumerMessage
	processedMessages chan *sarama.ConsumerMessage
	errors            chan *model.DispatcherMessage
	replies           chan *model.DispatcherMessage
	callback          model.DispatcherMessageConsumerCallback
	isCollectResult   bool
	ctx               context.Context
}

func (poolService workerPoolService) MakeWorkerPool(callback model.DispatcherMessageConsumerCallback, poolSize int, isCollectResult bool, ctx context.Context) WorkerPool {

	pool := poolService.new(callback, poolSize, isCollectResult, ctx)

	for i := 0; i < poolSize; i++ {
		go pool.newWorker(i)
	}

	go pool.sendBack(pool.errors, glob.ErrTopic)
	go pool.sendBack(pool.replies, glob.ReplyTopic)

	return pool
}

func (poolService workerPoolService) new(callback model.DispatcherMessageConsumerCallback, poolSize int, isCollectResult bool, ctx context.Context) workerPool {
	maxSize := 100000
	return workerPool{
		jobs:              make(chan *sarama.ConsumerMessage),
		processedMessages: make(chan *sarama.ConsumerMessage, maxSize),
		errors:            make(chan *model.DispatcherMessage, maxSize),
		replies:           make(chan *model.DispatcherMessage, maxSize),
		callback:          callback,
		isCollectResult:   isCollectResult,
		ctx:               ctx,
	}
}

func (p workerPool) AddJob(job *sarama.ConsumerMessage) {
	p.jobs <- job
}

func (p workerPool) Processed() <-chan *sarama.ConsumerMessage {
	return p.processedMessages
}

func (p workerPool) Context() context.Context {
	return p.ctx
}

func (p workerPool) newWorker(id int) {

	workerID := fmt.Sprintf("%v-%v", core.ProjectName, id)
	core.Logger.Debugf("Worker [%v] starts working ...", workerID)

	for {
		select {
		case job := <-p.jobs:
			p.doJob(workerID, job)
		case <-p.ctx.Done():
			core.Logger.Debugf("(Worker[%v]) cancelled", workerID)
			return
		}
	}
}

func (p workerPool) doJob(workerID string, saramaMsg *sarama.ConsumerMessage) {

	core.Logger.Debugf("(Worker[%v]) Starting work [%v-%v-%v/%v] ...", workerID, saramaMsg.Topic, saramaMsg.Partition, saramaMsg.Offset, glob.TrimBytes(saramaMsg.Value))

	// Mark offset after message processed / any error occurs
	defer func() { p.processedMessages <- saramaMsg }()

	// TODO: consume callback 吃 []byte 改為吃 *sarama.ConsumerMessage, 解析轉成DispatcherMessage

	// Parse
	message, err := p.parse(saramaMsg)
	if err != nil {
		err = errors.Wrapf(err, "error parsing message: %v", string(saramaMsg.Value))
		//core.Logger.Error(err.Error())
		return
	}

	// Send reply
	if message.IsReplyMessage {
		p.replies <- &message
	}

	// Process message
	if p.callback != nil {
		p.processMessage(workerID, message)
	}
}

func (p workerPool) parse(saramaMsg *sarama.ConsumerMessage) (dispatcherMsg model.DispatcherMessage, err error) {
	err = json.Unmarshal(saramaMsg.Value, &dispatcherMsg)
	if err != nil {
		return
	}

	// TODO: 移除相容舊版訊息格式
	if dispatcherMsg.TaskID == "" {
		// 解析失敗, 嘗試使用舊版格式 (可能為 model.ConsumerCallbackError 或 []byte)
		// try parsing to model.ConsumerCallbackError
		var tmp model.ConsumerCallbackError
		err = json.Unmarshal(saramaMsg.Value, &tmp)
		if tmp.ErrStr != "" {
			dispatcherMsg.ConsumerErrorStr = tmp.ErrStr
			dispatcherMsg.Value = tmp.Message.Value
		} else {
			// original message is []byte
			dispatcherMsg.Value = saramaMsg.Value
		}
	}

	dispatcherMsg.ConsumerReceivedTime = time.Now()
	dispatcherMsg.Offset = saramaMsg.Offset
	dispatcherMsg.Partition = saramaMsg.Partition
	return
}

func (p workerPool) sendBack(messagesChan chan *model.DispatcherMessage, getTopic func(oriTopic string) string) {
	for message := range messagesChan {
		// 僅回送一次
		message.IsSendError = false
		message.IsReplyMessage = false

		// 發送訊息
		message.Topic = getTopic(message.Topic)
		err := ProducerService.send(message)
		if err != nil {
			core.Logger.Error("Err sending back to producer:", err.Error())
		}
	}
}

func (p workerPool) done(job *sarama.ConsumerMessage) {
	p.processedMessages <- job
}

func (p workerPool) processMessage(workerID string, message model.DispatcherMessage) {

	// Catch panic on custom callback
	defer func() {
		if err := recover(); err != nil {
			core.Logger.Errorf("(Worker[%v]) Panic on user's callback: %v, stack trace: \n%v", workerID, err, string(debug.Stack()))
		}
	}()

	err := p.callback(message)

	// Send error message
	if err != nil && (message.IsSendError || message.TaskID == "" /* TODO: 相容舊版 */) {
		message.ConsumerErrorStr = err.Error()
		p.errors <- &message
		core.Logger.Debugf("(Worker[%v]) Error doing work [%v-%v-%v/%v]: %v", workerID, message.Topic, message.Partition, message.Offset, glob.TrimBytes(message.Value), err.Error())
	}
}
