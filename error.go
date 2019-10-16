package dispatcher

import "gitlab.paradise-soft.com.tw/glob/dispatcher/model"

func IsErrSubscribeOnExistedTopic(err error) bool {
	return err == model.ErrSubscribeOnSubscribedTopic
}
