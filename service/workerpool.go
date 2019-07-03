package service

import (
	"encoding/json"
	"fmt"
	"runtime/debug"

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
	Results() <-chan *sarama.ConsumerMessage
}

type workerPool struct {
	jobs         chan *sarama.ConsumerMessage
	results      chan *sarama.ConsumerMessage
	errors       chan *model.ConsumerCallbackError
	callback     model.ConsumerCallback
	isResultPool bool
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

func (p workerPool) worker(id int) {

	workerID := fmt.Sprintf("%v-%v", core.ProjName, id)
	core.Logger.Debugf("Worker [%v] starts working ...", workerID)

	defer func() {
		if err := recover(); err != nil {
			core.Logger.Errorf("(Worker[%v] Panic on user's callback: %v, stack trace: \n%v", workerID, err, string(debug.Stack()))
		}
	}()

	for job := range p.jobs {
		p.doJob(workerID, job)
	}
}

func (p workerPool) doJob(workerID string, job *sarama.ConsumerMessage) {

	core.Logger.Debugf("(Worker[%v]) Starting work [%v-%v-%v/%v] ...", workerID, job.Topic, job.Partition, job.Offset, glob.TrimBytes(job.Value))

	if p.callback != nil {
		err := p.callback(job.Value)
		if err != nil {
			p.errors <- &model.ConsumerCallbackError{Message: job, ErrStr: err.Error()}
			core.Logger.Debugf("(Worker[%v]) Error doing work [%v-%v-%v/%v]: %v", workerID, job.Topic, job.Partition, job.Offset, glob.TrimBytes(job.Value), err.Error())
		}
	}

	if p.isResultPool {
		p.results <- job
	}
	core.Logger.Debugf("(Worker[%v]) Finished work [%v-%v-%v/%v].", workerID, job.Topic, job.Partition, job.Offset, glob.TrimBytes(job.Value))
}

func (p workerPool) errSender() {
	core.Logger.Debug("Err worker starts working ...")
	for e := range p.errors {
		bytes, _ := json.Marshal(e)
		ProducerService.send(glob.ErrTopic(e.Message.Topic), bytes, false)
	}
}
