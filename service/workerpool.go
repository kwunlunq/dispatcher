package service

import (
	"strconv"

	"gitlab.paradise-soft.com.tw/dwh/dispatcher/model"

	"gitlab.paradise-soft.com.tw/dwh/dispatcher/glob"

	"github.com/Shopify/sarama"
	"gitlab.paradise-soft.com.tw/backend/yaitoo/tracer"
)

var WorkerPoolService = workerPoolService{}

type workerPoolService struct{}

type WorkerPool interface {
	AddJob(job *sarama.ConsumerMessage)
	Results() <-chan *sarama.ConsumerMessage
}

type workerPool struct {
	jobs     chan *sarama.ConsumerMessage
	results  chan *sarama.ConsumerMessage
	callback model.ConsumerCallback
}

func (this workerPoolService) MakeWorkerPool(callback model.ConsumerCallback, poolSize int) WorkerPool {
	w := workerPool{
		jobs:     make(chan *sarama.ConsumerMessage, poolSize),
		results:  make(chan *sarama.ConsumerMessage, poolSize),
		callback: callback,
	}
	for i := 0; i < poolSize; i++ {
		go w.worker(i)
	}
	return w
}

func (p workerPool) AddJob(job *sarama.ConsumerMessage) {
	p.jobs <- job
}

func (p workerPool) Results() <-chan *sarama.ConsumerMessage {
	return p.results
}

func (p workerPool) worker(id int) {

	workerID := glob.ProjName + strconv.Itoa(id)
	tracer.Trace(workerID, " Worker starts working ...")

	// Avoid explosion
	defer func() {
		if err := recover(); err != nil {
			tracer.Errorf(workerID, " Panic invoking user's callback: %v", err)
		}
	}()

	// Process message
	for job := range p.jobs {
		tracer.Tracef(workerID, " Starting work [%v/%v] ...", string(job.Key[:]), string(job.Value[:]))
		err := p.callback(job.Key, job.Value)
		if err != nil {
			tracer.Errorf(workerID, " Error doing work [%v/%v]: %v", string(job.Key[:]), string(job.Value[:]), err.Error())
		} else {
			tracer.Tracef(workerID, " Finished work [%v/%v]", string(job.Key[:]), string(job.Value[:]))
		}
		p.results <- job
	}
}
