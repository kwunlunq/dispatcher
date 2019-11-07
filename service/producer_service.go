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

	// Create task
	dis := model.MakeDispatcher(opts)
	task := model.NewTask(topic, value, dis)

	// Listen replies from consumer
	if dis.ProducerReplyHandler != nil {
		go p.replyMessageListener(task)
	}

	// Listen errors from consumer
	if dis.ProducerErrHandler != nil {
		go p.consumerListener(glob.ErrTopic(topic), wrapProducerErrHandler(dis.ProducerErrHandler))
	}

	// Send message
	err = p.send(task.Message)
	if err != nil {
		return
	}

	return
}

func (p *producerService) send(message model.Message) (err error) {

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

	// Send message via sarama api
	err = p.sendSaramaMessage(message)
	return
}

func (p *producerService) sendSaramaMessage(message model.Message) (err error) {
	// Close producer if panic
	defer func() {
		if r := recover(); r != nil {
			core.Logger.Errorf("Closing producer due to panic: %v", r)
			p.close()
		}
	}()

	// Prepare sarama message
	var messageBytes []byte
	messageBytes, err = json.Marshal(message)
	if err != nil {
		err = wraperrors.Wrap(err, "err converting Task on sending")
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
			// Create/Get client
			var client sarama.Client
			client, err = ClientService.Get()
			if err != nil {
				err = wraperrors.Wrap(err, "create producer error")
				core.Logger.Errorf("Error creating Producer: %s", err)
				return
			}
			// Create producer
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

func wrapProducerErrHandler(producerErrHandler model.ProducerCustomerErrHandler) model.MessageConsumerCallback {
	return func(message model.Message) error {
		defer func() {
			if err := recover(); err != nil {
				core.Logger.Errorf("Panic on custom producer err handler: %v", string(debug.Stack()))
			}
		}()
		producerErrHandler(message.Value, errors.New(message.ConsumerErrorStr))
		return nil
	}
}

// replyMessageListener 建立 & 執行 reply 任務, 並監控過期任務
func (p *producerService) replyMessageListener(task model.Task) {
	// Add task
	p.replyTasks.Store(task.Message.TaskID, task)
	core.Logger.Debug("Task added: ", task.Message.TaskID)

	// Check and remove expired task (start only once)
	go p.cleanerOnce.Do(func() { p.cleanExpiredTasks() })

	// Start consuming reply messages
	go p.consumerListener(glob.ReplyTopic(task.Message.Topic), p.handleReplyMessage())
}

// consumerListener 監聽來自consumer的訊息 (error & reply)
func (p *producerService) consumerListener(topic string, handler model.MessageConsumerCallback) {
	retryLimit := 5
	getRetryDuration := func(failCount int) time.Duration { return time.Second }
	err := ConsumerService.subscribeWithRetryMessageCallback(topic, handler, retryLimit, getRetryDuration)
	if err != nil {
		if wraperrors.Cause(err) != model.ErrSubscribeOnSubscribedTopic {
			core.Logger.Error("Error listening on consumer's message:", err.Error())
		} else {
			core.Logger.Debug("Error listening on consumer's message:", err.Error())
		}
		return
	}
}

// handleReplyMessage 執行handler, 移出tasks
func (p *producerService) handleReplyMessage() model.MessageConsumerCallback {
	return func(message model.Message) error {
		// Load task from cache by taskID to retrieve reply handler
		taskI, ok := p.replyTasks.Load(message.TaskID)
		if !ok {
			core.Logger.Warnf("Task not found, taskID: %v, value: %v", message.TaskID, glob.TrimBytes(message.Value))
			return nil
		}
		task := taskI.(model.Task)

		// Process
		task.Message = message
		message.ProducerReceivedTime = time.Now()
		p.runReplyHandler(task, nil)
		return nil
	}
}

// cleanExpiredTasks 每5秒鐘檢查一次, 移除超時的任務(時間為使用者自訂)
func (p *producerService) cleanExpiredTasks() func() {
	checkInterval := 5 * time.Second
	for {
		//core.Logger.Debug("正在檢查過期tasks...")
		now := time.Now().UnixNano()
		deletedCount := 0
		count := 0
		p.replyTasks.Range(func(key, value interface{}) bool {
			count++
			task, ok := value.(model.Task)
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
func (p *producerService) runReplyHandler(task model.Task, err error) {
	defer func() {
		if err := recover(); err != nil {
			core.Logger.Errorf("Panic on custom reply handler: %v", string(debug.Stack()))
		}
	}()
	task.ReplyHandler(task.Message, err)
	p.replyTasks.Delete(task.Message.TaskID)
	core.Logger.Debug("Task removed: ", task.Message.TaskID)
}
