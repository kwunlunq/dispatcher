package service

import (
	"gitlab.paradise-soft.com.tw/dwh/dispatcher/glob/core"
	"sync"

	"github.com/Shopify/sarama"
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
	client, err = sarama.NewClient(core.Config.Brokers, &core.SaramaConfig)

	if err != nil {
		core.Logger.Errorf("Error creating client: %v", err.Error())
		return
	}
	core.Logger.Debugf("Client created.")
	return
}
