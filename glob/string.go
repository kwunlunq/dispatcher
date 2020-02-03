package glob

import (
	"crypto/md5"
	"fmt"
	"gitlab.paradise-soft.com.tw/glob/dispatcher/glob/core"
	"strings"
)

func HashMd5(str string) string {
	return fmt.Sprintf("%x", md5.Sum([]byte(str)))
}

func FormatGroupID(topic, groupID string) (formattedGroupID string) {
	formattedGroupID = groupID
	if strings.TrimSpace(formattedGroupID) == "" {
		formattedGroupID = core.Config.DefaultGroupID
	}
	formattedGroupID = AppendSuffix(formattedGroupID, topic, ":")
	return
}
