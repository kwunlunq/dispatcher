package service

import (
	"github.com/pkg/errors"
	"gitlab.paradise-soft.com.tw/glob/dispatcher/glob/core"
	"sync"

	"github.com/Shopify/sarama"
)

type clientService struct {
	client sarama.Client
	lock   *sync.Mutex
}

var ClientService = &clientService{lock: &sync.Mutex{}}

func (s *clientService) GetNew() (client sarama.Client, err error) {
	return s.create()
}

func (s *clientService) Get() (client sarama.Client, err error) {
	if s.client == nil {
		s.lock.Lock()
		defer s.lock.Unlock()
		if s.client == nil {
			client, err = s.create()
			if err != nil {
				err = errors.Wrap(err, "err getting client")
				core.Logger.Error(err.Error())
				return
			}
			s.client = client
		}
	}
	client = s.client
	return
}

func (s *clientService) Reconnect() {
	err := s.client.Close()
	if err != nil {
		core.Logger.Error("Err closing client during reconnecting: " + err.Error())
		return
	}
	newClient, err := s.create()
	if err != nil {
		core.Logger.Error("Err creating client: " + err.Error())
		return
	}
	s.client = newClient
	core.Logger.Info("kafka client reconnected")
}

func (s *clientService) RefreshMetadata() {
	if s.client == nil {
		return
	}
	err := s.client.RefreshMetadata()
	if err != nil {
		core.Logger.Error("Error refreshing metadata: ", err)
	}
}

func (s *clientService) create() (client sarama.Client, err error) {
	// TODO: add timeout
	client, err = sarama.NewClient(core.Config.Brokers, &core.SaramaConfig)

	if err != nil {
		err = errors.Wrap(err, "err creating client")
		core.Logger.Error(err.Error())
		return
	}
	core.Logger.Debugf("Client created.")
	return
}
