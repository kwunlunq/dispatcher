package model

import (
	"github.com/google/uuid"
	"sync"
	"time"
)

type Task struct {
	TaskInfo
	TaskHandler
	Message Message
}

type TaskInfo struct {
	CreatedTime     time.Time
	ExpiredTimeNano int64
}

type TaskHandler struct {
	ReplyHandler func(message Message, err error)
}

// NewTask 利用 topic, message, dispatcher設定等 包裝成dispatcher用的task物件
func NewTask(topic string, message []byte, dis Dispatcher) (task Task) {
	var messageKey string // TODO: 未來 message key 可由客戶端決定
	if dis.ProducerEnsureOrder {
		messageKey = topic
	}
	if dis.ProducerMessageKey != "" {
		messageKey = dis.ProducerMessageKey
	}
	var onceReplyHandler sync.Once
	task = Task{
		Message: Message{
			TaskID:           uuid.New().String(),
			Topic:            topic,
			Key:              messageKey,
			Value:            message,
			ProducerSentTime: time.Now(),
			IsSendError:      dis.ProducerErrHandler != nil,
			IsReplyMessage:   dis.ProducerReplyHandler != nil,
		},
		TaskInfo: TaskInfo{
			CreatedTime:     time.Now(),
			ExpiredTimeNano: time.Now().Add(dis.ProducerReplyTimeout).UnixNano(),
		},
		TaskHandler: TaskHandler{
			ReplyHandler: func(message Message, err error) {
				onceReplyHandler.Do(func() { dis.ProducerReplyHandler(message, err) })
			},
		},
	}
	return
}
