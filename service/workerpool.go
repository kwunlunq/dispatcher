package service

import (
	"encoding/json"
	"fmt"
	"runtime/debug"

	"gitlab.paradise-soft.com.tw/dwh/dispatcher/model"

	"gitlab.paradise-soft.com.tw/dwh/dispatcher/glob"

	"github.com/Shopify/sarama"
)

var (
	WorkerPoolService = workerPoolService{}
)

type workerPoolService struct{}

type WorkerPool interface {
	AddJob(job *sarama.ConsumerMessage)
	Results() <-chan *sarama.ConsumerMessage
	// Errors() <-chan *model.ConsumerCallbackError
}

type workerPool struct {
	jobs         chan *sarama.ConsumerMessage
	results      chan *sarama.ConsumerMessage
	errors       chan *model.ConsumerCallbackError
	callback     model.ConsumerCallback
	isResultPool bool
	logger       glob.GlobLogger
}

func (this workerPoolService) MakeWorkerPool(callback model.ConsumerCallback, poolSize int, isResultPool bool) WorkerPool {
	w := workerPool{
		jobs:         make(chan *sarama.ConsumerMessage, poolSize),
		results:      make(chan *sarama.ConsumerMessage, poolSize),
		errors:       make(chan *model.ConsumerCallbackError, 1000),
		callback:     callback,
		isResultPool: isResultPool,
	}
	for i := 0; i < poolSize; i++ {
		go w.worker(i)
	}
	go w.errSender()
	return w
}

func (p workerPool) AddJob(job *sarama.ConsumerMessage) {
	p.jobs <- job
}

func (p workerPool) Results() <-chan *sarama.ConsumerMessage {
	return p.results
}

// func (p workerPool) Errors() <-chan *model.ConsumerCallbackError {
// 	return p.errors
// }

func (p workerPool) worker(id int) {

	workerID := fmt.Sprintf("%v-%v", glob.ProjName, id)
	glob.Logger.Debugf(" Worker [%v] starts working ...", workerID)

	defer func() {
		if err := recover(); err != nil {
			glob.Logger.Errorf(" (Worker[%v] Panic on user's callback: %v, stack trace: \n%v", workerID, err, string(debug.Stack()))
		}
	}()

	for job := range p.jobs {
		p.doJob(workerID, job)
	}
}

func (p workerPool) doJob(workerID string, job *sarama.ConsumerMessage) {

	glob.Logger.Debugf(" (Worker[%v]) Starting work [%v-%v-%v/%v/%v] ...", workerID, job.Topic, job.Partition, job.Offset, string(job.Key[:]), glob.TrimBytes(job.Value))

	if p.callback != nil {
		err := p.callback(job.Key, job.Value)
		if err != nil {
			p.errors <- &model.ConsumerCallbackError{Message: job, ErrStr: err.Error()}
			glob.Logger.Debugf(" (Worker[%v]) Error doing work [%v-%v-%v/%v/%v]: %v", workerID, job.Topic, job.Partition, job.Offset, string(job.Key[:]), glob.TrimBytes(job.Value), err.Error())
		}
	}

	if p.isResultPool {
		p.results <- job
	}
	glob.Logger.Debugf(" (Worker[%v]) Finished work [%v-%v-%v/%v/%v].", workerID, job.Topic, job.Partition, job.Offset, string(job.Key[:]), glob.TrimBytes(job.Value))
}

func (p workerPool) errSender() {
	glob.Logger.Debug(" Err worker starts working ...")
	for e := range p.errors {
		bytes, _ := json.Marshal(e)
		// glob.Logger.Debugf(" Err worker sending: %v/%v/%v\n", glob.ErrTopic(e.Message.Topic), e.Message.Key, e)
		ProducerService.send(glob.ErrTopic(e.Message.Topic), []byte(e.Message.Key), bytes)
	}
}
