package service

import (
	"sync"

	"github.com/Shopify/sarama"
	"gitlab.paradise-soft.com.tw/backend/yaitoo/tracer"
	"gitlab.paradise-soft.com.tw/dwh/dispatcher/glob"
)

type clientService struct {
	client sarama.Client
	lock   *sync.Mutex
}

var ClientService = &clientService{lock: &sync.Mutex{}}

func (s *clientService) GetNew() sarama.Client {
	client, _ := s.create()
	return client
}

func (s *clientService) Get() sarama.Client {
	if s.client == nil {
		s.lock.Lock()
		if s.client == nil {
			client, err := s.create()
			if err == nil {
				s.client = client
			}
		}
		s.lock.Unlock()
	}
	return s.client
}

func (s *clientService) create() (client sarama.Client, err error) {
	client, err = sarama.NewClient(glob.Config.Brokers, &glob.SaramaConfig)

	if err != nil {
		tracer.Errorf(glob.ProjName, "Error creating client: %v", err.Error())
		return
	}
	tracer.Trace(glob.ProjName, " Client created.")
	return
}
