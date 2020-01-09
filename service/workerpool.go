package service

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"gitlab.paradise-soft.com.tw/glob/dispatcher/glob"
	"gitlab.paradise-soft.com.tw/glob/dispatcher/glob/core"
	"gitlab.paradise-soft.com.tw/glob/dispatcher/model"
	"runtime/debug"
	"time"

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
	ctx               context.Context
	callback          model.MessageConsumerCallback
	groupID           string
	jobs              chan *sarama.ConsumerMessage
	processedMessages chan *sarama.ConsumerMessage
	errors            chan model.Message
	replies           chan model.Message
}

func (poolService workerPoolService) MakeWorkerPool(ctx context.Context, poolSize int, callback model.MessageConsumerCallback, groupID string) WorkerPool {

	pool := poolService.new(ctx, poolSize, callback, groupID)

	for i := 0; i < poolSize; i++ {
		go pool.newWorker(i)
	}

	go pool.sendBack(pool.errors, glob.ErrTopic)
	go pool.sendBack(pool.replies, glob.ReplyTopic)

	return pool
}

func (poolService workerPoolService) new(ctx context.Context, poolSize int, callback model.MessageConsumerCallback, groupID string) workerPool {
	maxSize := 100000
	return workerPool{
		jobs:              make(chan *sarama.ConsumerMessage),
		processedMessages: make(chan *sarama.ConsumerMessage, maxSize),
		errors:            make(chan model.Message, maxSize),
		replies:           make(chan model.Message, maxSize),
		callback:          callback,
		ctx:               ctx,
		groupID:           groupID,
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

	core.Logger.Infof("(Worker[%v]) Received message from topic: [%v, %v-%v], msg: %v", workerID, saramaMsg.Topic, saramaMsg.Partition, saramaMsg.Offset, glob.TrimBytes(saramaMsg.Value))

	// Mark offset after message processed / any error occurs
	defer func() { p.processedMessages <- saramaMsg }()

	// Parse
	message, err := p.parse(saramaMsg)
	if err != nil {
		err = errors.Wrapf(err, "error parsing message: %v", string(saramaMsg.Value))
		core.Logger.Error(err.Error())
		return
	}
	core.Logger.Debug("Message received: ", message.Debug())

	// Send reply
	if message.IsReplyMessage {
		p.replies <- message
	}

	// Process message
	if p.callback != nil {
		p.processMessage(workerID, message)
	}
}

// parse 解析收到訊息, 目前因相容舊版訊息格式, 不會有error發生 (解析失敗時放進message.Value)
func (p workerPool) parse(saramaMsg *sarama.ConsumerMessage) (message model.Message, err error) {
	err = json.Unmarshal(saramaMsg.Value, &message)
	if err != nil {
		core.Logger.Error("Err parsing json message: ", string(saramaMsg.Value))
		return
	}

	// TODO: 相容舊版訊息格式
	p.parseCompatibleMessage(&message, saramaMsg.Value)

	message.Offset = saramaMsg.Offset
	message.Partition = saramaMsg.Partition
	message.Topic = saramaMsg.Topic
	// 加上時間戳
	if message.ConsumerReceivedTime.IsZero() { // it's consumer consuming message
		message.ConsumerReceivedTime = time.Now()
		message.ConsumerGroupID = p.groupID
	} else if message.ProducerReceivedTime.IsZero() { // it's producer consuming message
		message.ProducerReceivedTime = time.Now()
	}
	return
}

func (p workerPool) sendBack(messagesChan chan model.Message, getTopic func(oriTopic string) string) {
	for message := range messagesChan {
		// 僅回送一次
		message.IsSendError = false
		message.IsReplyMessage = false

		// 發送訊息
		message.Topic = getTopic(message.Topic)
		err := ProducerService.send(message)
		core.Logger.Infof("Message sent back: Topic [%v], Message [%v]", message.Topic, string(message.Value))
		if err != nil {
			core.Logger.Error("Err sending back to producer:", err.Error())
		}
	}
}

func (p workerPool) done(job *sarama.ConsumerMessage) {
	p.processedMessages <- job
}

func (p workerPool) processMessage(workerID string, message model.Message) {

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
		p.errors <- message
		core.Logger.Debugf("(Worker[%v]) Error handling message [%v-%v-%v/%v]: %v", workerID, message.Topic, message.Partition, message.Offset, glob.TrimBytes(message.Value), err.Error())
	}
}

func (p workerPool) parseCompatibleMessage(message *model.Message, saramaMsgVal []byte) {
	if message.TaskID == "" {
		// 舊版error-message格式: model.ConsumerCallbackError
		var tmp model.ConsumerCallbackError
		_ = json.Unmarshal(saramaMsgVal, &tmp)
		if tmp.ErrStr != "" {
			message.ConsumerErrorStr = tmp.ErrStr
			message.Value = tmp.Message.Value
		} else {
			// 舊版message格式: []byte
			message.Value = saramaMsgVal
		}
	}
}
