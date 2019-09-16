package service

import (
	"context"
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
	Context() context.Context
}

type workerPool struct {
	jobs            chan *sarama.ConsumerMessage
	results         chan *sarama.ConsumerMessage
	errors          chan *model.ConsumerCallbackError
	callback        model.ConsumerCallback
	isCollectResult bool
	ctx             context.Context
}

func (poolService workerPoolService) MakeWorkerPool(callback model.ConsumerCallback, poolSize int, isCollectResult bool, ctx context.Context) WorkerPool {

	pool := poolService.new(callback, poolSize, isCollectResult, ctx)

	for i := 0; i < poolSize; i++ {
		go pool.newWorker(i)
	}

	go pool.errSender()

	return pool
}

func (poolService workerPoolService) new(callback model.ConsumerCallback, poolSize int, isCollectResult bool, ctx context.Context) workerPool {
	return workerPool{
		jobs:            make(chan *sarama.ConsumerMessage, poolSize),
		results:         make(chan *sarama.ConsumerMessage, poolSize),
		errors:          make(chan *model.ConsumerCallbackError, 1000),
		callback:        callback,
		isCollectResult: isCollectResult,
		ctx:             ctx,
	}
}

func (p workerPool) AddJob(job *sarama.ConsumerMessage) {
	p.jobs <- job
}

func (p workerPool) Results() <-chan *sarama.ConsumerMessage {
	return p.results
}

func (p workerPool) Context() context.Context {
	return p.ctx
}

func (p workerPool) newWorker(id int) {

	workerID := fmt.Sprintf("%v-%v", core.ProjectName, id)
	core.Logger.Debugf("Worker [%v] starts working ...", workerID)

	defer func() {
		if err := recover(); err != nil {
			core.Logger.Errorf("(Worker[%v]) Panic on user's callback: %v, stack trace: \n%v", workerID, err, string(debug.Stack()))
		}
	}()

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

	if p.callback != nil {
		err := p.callback(job.Value)
		if err != nil {
			p.errors <- &model.ConsumerCallbackError{Message: job, ErrStr: err.Error()}
			core.Logger.Debugf("(Worker[%v]) Error doing work [%v-%v-%v/%v]: %v", workerID, job.Topic, job.Partition, job.Offset, glob.TrimBytes(job.Value), err.Error())
		}
	}

	if p.isCollectResult {
		p.results <- job
	}
	core.Logger.Debugf("(Worker[%v]) Finished work [%v-%v-%v/%v].", workerID, job.Topic, job.Partition, job.Offset, glob.TrimBytes(job.Value))
}

func (p workerPool) errSender() {
	core.Logger.Debug("Err newWorker starts working ...")
	for e := range p.errors {
		bytes, _ := json.Marshal(e)
		err := ProducerService.send(glob.ErrTopic(e.Message.Topic), bytes, false)
		if err != nil {
			core.Logger.Error("Err sending back error:", err.Error())
		}
	}
}
