package model

import (
	"github.com/google/uuid"
	"sync"
	"time"
)

type DispatcherTask struct {
	DispatcherTaskInfo
	DispatcherTaskHandler
	Message DispatcherMessage
}

type DispatcherMessage struct {
	TaskID               string
	Topic                string
	Key                  string
	Value                []byte
	Partition            int32
	Offset               int64
	ConsumerErrorStr     string
	ProducerSentTime     time.Time
	ConsumerReceivedTime time.Time
	ConsumerFinishTime   time.Time
	ProducerReceivedTime time.Time
	IsReplyMessage       bool
	IsSendError          bool
	IsSendBack           bool
}

type DispatcherTaskInfo struct {
	CreatedTime     time.Time
	ExpiredTimeNano int64
}

type DispatcherTaskHandler struct {
	ReplyHandler func(message DispatcherMessage, err error)
}

// NewDispatcherTask 利用 topic, message, dispatcher設定等 包裝成dispatcher用的task物件
func NewDispatcherTask(topic string, message []byte, dis Dispatcher) (task DispatcherTask) {
	var messageKey string // TODO: 未來 message key 可由客戶端決定
	if dis.ProducerEnsureOrder {
		messageKey = topic
	}
	if dis.ProducerMessageKey != "" {
		messageKey = dis.ProducerMessageKey
	}
	var onceReplyHandler sync.Once
	task = DispatcherTask{
		Message: DispatcherMessage{
			TaskID:           uuid.New().String(),
			Topic:            topic,
			Key:              messageKey,
			Value:            message,
			ProducerSentTime: time.Now(),
			IsSendError:      dis.ProducerErrHandler != nil,
			IsReplyMessage:   dis.ProducerReplyHandler != nil,
		},
		DispatcherTaskInfo: DispatcherTaskInfo{
			CreatedTime:     time.Now(),
			ExpiredTimeNano: time.Now().Add(dis.ProducerReplyTimeout).UnixNano(),
		},
		DispatcherTaskHandler: DispatcherTaskHandler{
			ReplyHandler: func(dispatcherMessage DispatcherMessage, err error) {
				onceReplyHandler.Do(func() { dis.ProducerReplyHandler(dispatcherMessage, err) })
			},
		},
	}
	return
}
