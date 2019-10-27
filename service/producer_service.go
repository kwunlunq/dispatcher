package service

import (
	"encoding/json"
	"errors"
	"gitlab.paradise-soft.com.tw/glob/dispatcher/glob"
	"runtime/debug"
	"sync"
	"time"

	"gitlab.paradise-soft.com.tw/glob/dispatcher/glob/core"

	"gitlab.paradise-soft.com.tw/glob/dispatcher/model"

	wraperrors "github.com/pkg/errors"

	"github.com/Shopify/sarama"
)

var ProducerService = &producerService{lock: &sync.Mutex{}}

type producerService struct {
	producer    sarama.AsyncProducer
	lock        *sync.Mutex
	replyTasks  sync.Map
	cleanerOnce sync.Once
}

func (p *producerService) Send(topic string, value []byte, opts ...model.Option) (err error) {
	if !core.IsInitialized() {
		return model.ErrNotInitialized
	}

	// Send message
	dis := model.MakeDispatcher(opts)
	task := model.NewDispatcherTask(topic, value, dis)
	err = p.send(&task.Message)
	if err != nil {
		return
	}

	// TODO: 整合 err, reply handler

	// Listen errors from consumer
	if dis.ProducerErrHandler != nil {
		go p.listenErrorFromConsumer(topic, dis.ProducerErrHandler)
	}

	// Listen reply from consumer
	if dis.ProducerReplyHandler != nil {
		go p.listenReplyFromConsumer(topic, task)
	}
	return
}

func (p *producerService) send(message *model.DispatcherMessage) (err error) {

	// Create topic
	err = TopicService.Create(message.Topic)
	if err != nil {
		return
	}

	// Get/Create producer
	err = p.createProducer()
	if err != nil {
		return
	}

	// Close producer if panic
	defer func() {
		if r := recover(); r != nil {
			core.Logger.Errorf("Closing producer due to panic: %v", r)
			p.close()
		}
	}()

	// Send message
	var messageBytes []byte
	messageBytes, err = json.Marshal(message)
	if err != nil {
		err = wraperrors.Wrap(err, "err converting DispatcherTask on sending")
		return
	}
	saramaMessage := &sarama.ProducerMessage{Topic: message.Topic, Value: sarama.ByteEncoder(messageBytes)}
	if message.Key != "" {
		saramaMessage.Key = sarama.StringEncoder(message.Key)
	}

	// Send
	p.producer.Input() <- saramaMessage

	// Wait ack
	select {
	case saramaMessage = <-p.producer.Successes():
		message.Partition = saramaMessage.Partition
		message.Offset = saramaMessage.Offset
		core.Logger.Debugf("Message sent: topic: %v, partition: %v, offset: %v, key: %v, message-val: %v", message.Topic, message.Partition, message.Offset, message.Key, glob.TrimBytes(message.Value))
		return
	case err = <-p.producer.Errors():
		core.Logger.Errorf("Failed to produce message: %v, len: %v", err, len(messageBytes))
		return
	}
}

func (p *producerService) createProducer() (err error) {
	if p.producer == nil {
		p.lock.Lock()
		defer p.lock.Unlock()
		if p.producer == nil {
			var client sarama.Client
			client, err = ClientService.Get()
			if err != nil {
				err = wraperrors.Wrap(err, "create producer error")
				core.Logger.Errorf("Error creating Producer: %s", err)
				return
			}
			p.producer, err = sarama.NewAsyncProducerFromClient(client)
			core.Logger.Debugf("Producer created.")
			if err != nil {
				err = wraperrors.Wrap(err, "create producer error")
				core.Logger.Errorf("Error creating Producer: %s", err)
				return
			}
		}
	}
	return
}

func (p *producerService) close() {
	p.producer = nil
	if err := p.producer.Close(); err != nil {
		core.Logger.Errorf("Error closing producer: %v", err.Error())
	}
}

func (p *producerService) listenErrorFromConsumer(topic string, errHandler model.ProducerCustomerErrHandler) {
	c, err := ConsumerService.Subscribe(glob.ErrTopic(topic), handleConsumerError(errHandler))
	if err != nil {
		if wraperrors.Cause(err) != model.ErrSubscribeOnSubscribedTopic {
			core.Logger.Error("Error creating consumer:", err.Error())
		}
		return
	}
	consumeErr, _ := <-c.ConsumeErrChan
	if consumeErr != nil {
		core.Logger.Error("Error consuming:", consumeErr.Error())
	}
}

// listenReplyFromConsumer 建立 & 執行 reply 任務, 並監控過期任務
func (p *producerService) listenReplyFromConsumer(topic string, task model.DispatcherTask) {

	// Add task
	p.replyTasks.Store(task.Message.TaskID, task)
	//defer p.replyTasks.Delete(task.ID)

	// Check and remove expired task
	go p.cleanerOnce.Do(func() { p.cleanExpiredTasks() })

	// Start consuming reply messages
	go p.consumeReplyMessages(task)

	// Listen until timeout exceed
	//replyTask, err := p.listenReplyMessageUntilTimeout(task, dis.ProducerReplyTimeout)

	// Custom handle process
	//defer func() {
	//	if err := recover(); err != nil {
	//		core.Logger.Errorf("Panic on custom retry handler: %v", string(debug.Stack()))
	//	}
	//}()
	//dis.ProducerReplyHandler(replyTask, err)
}

func wrapProducerErrHandler(producerErrHandler model.ProducerCustomerErrHandler) model.DispatcherMessageConsumerCallback {
	return func(message model.DispatcherMessage) error {
		defer func() {
			if err := recover(); err != nil {
				core.Logger.Errorf("Panic on producer err handler: %v", string(debug.Stack()))
			}
		}()
		producerErrHandler(message.Value, errors.New(message.ConsumerErrorStr))
		return nil
	}
}

// consumeReplyMessages 監聽回條訊息 ( {topic}_Reply )
func (p *producerService) consumeReplyMessages(task model.DispatcherTask) {
	retryLimit := 10
	getRetryDuration := func(failCount int) time.Duration { return time.Duration(failCount*failCount) * time.Second }
	consumeErr := ConsumerService.SubscribeWithRetry(glob.ReplyTopic(task.Message.Topic), p.handleReplyMessage(), retryLimit, getRetryDuration)
	if consumeErr != nil {
		core.Logger.Error("Error consuming reply message: ", consumeErr.Error())
	}
}

// handleReplyMessage 執行handler, 移出tasks
func (p *producerService) handleReplyMessage() model.BytesConsumerCallback {
	return func(value []byte) error {
		// Parse
		var replyMessage model.DispatcherMessage
		parseErr := json.Unmarshal(value, &replyMessage)
		if parseErr != nil {
			core.Logger.Error("Error parsing reply message: ", parseErr.Error(), ", msg: ", string(value))
			return nil
		}

		// Load from cache
		taskI, ok := p.replyTasks.Load(replyMessage.TaskID)
		if !ok {
			core.Logger.Debugf("DispatcherTask not found, taskID: %v, value: %v", replyMessage.TaskID, glob.TrimBytes(replyMessage.Value))
			return nil
		}
		task := taskI.(model.DispatcherTask)

		// Process
		p.runReplyHandler(task, nil)

		//task.ResultChan <- replyMessageTask
		//close(task.ResultChan)
		return nil
	}
}

//func (p *producerService) listenReplyMessageUntilTimeout(task model.DispatcherTask, waitTimeout time.Duration) (replyMessage model.DispatcherTask, err error) {
//	select {
//	case replyMessage = <-task.ResultChan:
//	case <-time.NewTimer(waitTimeout).C:
//		item, ok := p.replyTasks.Load(task.ID)
//		if ok {
//			replyMessage = item.(model.DispatcherTask)
//		}
//		err = model.ErrTimeout
//		core.Logger.Error("Error listening reply from consumer:", err.Error())
//	}
//	return
//}

// cleanExpiredTasks 檢查並移出超時的任務
func (p *producerService) cleanExpiredTasks() func() {
	checkInterval := 5 * time.Second
	for {
		//core.Logger.Debug("正在檢查過期tasks...")
		now := time.Now().UnixNano()
		deletedCount := 0
		count := 0
		p.replyTasks.Range(func(key, value interface{}) bool {
			count++
			task, ok := value.(model.DispatcherTask)
			if !ok {
				return true
			}
			if task.ExpiredTimeNano >= 0 && now > task.ExpiredTimeNano {
				p.runReplyHandler(task, model.ErrTimeout)
				deletedCount++
			}
			return true
		})
		//core.Logger.Debugf("檢查完畢: 總共%d筆, 刪除%d筆", count, deletedCount)
		if deletedCount > 0 {
			core.Logger.Debugf("刪除過期的reply任務: %d筆, 總共 %d筆", deletedCount, count)
		}
		time.Sleep(checkInterval)
	}
}

// runReplyHandler run handler (only once) & remove the task
func (p *producerService) runReplyHandler(task model.DispatcherTask, err error) {
	task.ReplyHandler(task.Message, err)
	p.replyTasks.Delete(task.Message.TaskID)
}
