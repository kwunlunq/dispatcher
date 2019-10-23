package dispatcher

import (
	"github.com/pkg/errors"
	"gitlab.paradise-soft.com.tw/glob/dispatcher/model"
)

func IsErrSubscribeOnExistedTopic(err error) bool {
	return errors.Cause(err) == model.ErrSubscribeOnSubscribedTopic
}
