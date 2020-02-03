package service

import (
	"errors"
	"fmt"
	"github.com/parnurzeal/gorequest"
	"gitlab.paradise-soft.com.tw/glob/dispatcher/glob"
	"gitlab.paradise-soft.com.tw/glob/dispatcher/glob/core"
	"gitlab.paradise-soft.com.tw/glob/dispatcher/model"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

var (
	ConsumeStatusService   consumeStatusService
	ErrMonitorHostNotSet   = errors.New("err monitor host not set (set by InitSetMonitorHost)")
	_regexMonitorKafkaLags = regexp.MustCompile("[0-9]+")
)

type consumeStatusService struct{}

// Get lag count from API
func (c consumeStatusService) Get(topic, groupID string) (status model.ConsumeStatus, err error) {
	groupID = glob.FormatGroupID(topic, groupID)
	status.GroupID = groupID
	if strings.TrimSpace(core.Config.MonitorHost) == "" {
		err = ErrMonitorHostNotSet
		return
	}
	resp, body, errs := gorequest.New().Get(core.Config.MonitorHost + "monitor/kafka/lags").
		Query("topic=" + url.QueryEscape(topic)).
		Query("group_id=" + url.QueryEscape(groupID)).
		End()
	if len(errs) > 0 {
		err = errs[0]
		return
	}
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusBadRequest {
			err = fmt.Errorf("error: %v, on requesting %v", errors.New(string(body)), resp.Request.URL.String())
			return
		} else {
			err = fmt.Errorf("invalid status code: %v, on requesting %v", strconv.Itoa(resp.StatusCode), resp.Request.URL.String())
			return
		}
	}
	status.LagCount, err = strconv.Atoi(_regexMonitorKafkaLags.FindString(string(body)))
	return
}

func (c consumeStatusService) List(topic string) (result []model.ConsumeStatus) {
	return
}
