package model

import (
	"github.com/google/uuid"
	"gitlab.paradise-soft.com.tw/glob/dispatcher/glob/core"
	"runtime/debug"
	"sync"
	"time"
)

type Task struct {
	TaskInfo
	Replier
	Message Message
}

func (t Task) IsExpired(checkStartTime int64) bool {
	return t.ExpiredTimeNano > 0 && checkStartTime > t.ExpiredTimeNano
}

type TaskInfo struct {
	CreatedTime     time.Time
	ExpiredTimeNano int64
}

type Replier struct {
	Handler    func(message Message, err error)
	IsExecuted bool
	IsExpired  bool
	Locker     *sync.RWMutex
}

func (r *Replier) HandleMessage(message Message, err error) {
	core.Logger.Debugf("Executing reply handler: %v", string(message.Value))
	defer func() {
		if err := recover(); err != nil {
			core.Logger.Errorf("Panic on custom reply handler: %v\nsStacktrace:\n%v", err, string(debug.Stack()))
		}
	}()
	r.Locker.Lock()
	if !r.IsExpired {
		r.IsExecuted = true
		go r.Handler(message, err)
	}
	r.Locker.Unlock()
}

// NewTask 利用 topic, message, dispatcher設定等 包裝成dispatcher用的task物件
func NewTask(topic string, message []byte, dis Dispatcher) (task *Task) {
	var messageKey string
	var expiredTimeNano int64
	if dis.ProducerReplyTimeout > 0 {
		expiredTimeNano = time.Now().Add(dis.ProducerReplyTimeout).UnixNano()
	}
	if dis.ProducerEnsureOrder {
		messageKey = topic
	}
	if dis.ProducerMessageKey != "" {
		messageKey = dis.ProducerMessageKey
	}
	task = &Task{
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
			ExpiredTimeNano: expiredTimeNano,
		},
		Replier: Replier{
			Handler: func(message Message, err error) {
				dis.ProducerReplyHandler(message, err)
			},
			Locker: &sync.RWMutex{},
		},
	}
	return
}
