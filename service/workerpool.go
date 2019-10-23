package service

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime/debug"
	"time"

	"gitlab.paradise-soft.com.tw/glob/dispatcher/model"

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
	callback          model.ConsumerCallback
	isCollectResult   bool
	ctx               context.Context
}

func (poolService workerPoolService) MakeWorkerPool(callback model.ConsumerCallback, poolSize int, isCollectResult bool, ctx context.Context) WorkerPool {

	pool := poolService.new(callback, poolSize, isCollectResult, ctx)

	for i := 0; i < poolSize; i++ {
		go pool.newWorker(i)
	}

	go pool.sendBack(pool.errors, glob.ErrTopic)
	go pool.sendBack(pool.replies, glob.ReplyTopic)

	return pool
}

func (poolService workerPoolService) new(callback model.ConsumerCallback, poolSize int, isCollectResult bool, ctx context.Context) workerPool {
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

func (p workerPool) doJob(workerID string, job *sarama.ConsumerMessage) {

	core.Logger.Debugf("(Worker[%v]) Starting work [%v-%v-%v/%v] ...", workerID, job.Topic, job.Partition, job.Offset, glob.TrimBytes(job.Value))

	// Mark offset after message processed / any error occurs
	defer func() { p.processedMessages <- job }()

	// Parse
	var message model.DispatcherMessage
	err := json.Unmarshal(job.Value, &message)
	if err != nil {
		core.Logger.Error("Error parsing message from producer", string(job.Value))
		return
	}

	message.ConsumerReceivedTime = time.Now()
	message.Offset = job.Offset
	message.Partition = job.Partition

	// Send reply
	if message.IsReplyMessage {
		p.replies <- &message
	}

	// Process message
	if p.callback != nil {
		p.processMessage(workerID, job, message)
	}
}

func (p workerPool) sendBack(messagesChan chan *model.DispatcherMessage, getTopic func(oriTopic string) string) {
	for message := range messagesChan {
		if !message.IsSendBack {
			continue
		}
		message.IsSendBack = false // 僅回送一次
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

func (p workerPool) processMessage(workerID string, job *sarama.ConsumerMessage, message model.DispatcherMessage) {

	// Custom callback
	defer func() {
		if err := recover(); err != nil {
			core.Logger.Errorf("(Worker[%v]) Panic on user's callback: %v, stack trace: \n%v", workerID, err, string(debug.Stack()))
		}
	}()
	err := p.callback(message.Value)

	// Send error message
	if err != nil && message.IsSendError {
		message.ConsumerErrorStr = err.Error()
		p.errors <- &message
		core.Logger.Debugf("(Worker[%v]) Error doing work [%v-%v-%v/%v]: %v", workerID, job.Topic, job.Partition, job.Offset, glob.TrimBytes(job.Value), err.Error())
	}
}
