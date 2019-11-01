package glob

import "strings"

func ErrTopic(topic string) string {
	return topic + "_ERR"
}

func ReplyTopic(topic string) string {
	return topic + "_Reply"
}

func TrimBytes(bytes []byte) string {
	str := string(bytes)
	if len(str) > 150 {
		return str[:150] + " ..."
	}
	return str
}

func AppendPrefix(str string, prefix string, ch string) string {
	if !strings.HasSuffix(prefix, ch) {
		prefix = prefix + ch
	}
	if strings.HasPrefix(str, prefix) {
		return str
	}
	return prefix + str
}

func AppendSuffix(str string, suffix string, ch string) string {
	if !strings.HasPrefix(suffix, ch) {
		suffix = ch + suffix
	}
	if strings.HasSuffix(str, suffix) {
		return str
	}
	return str + suffix
}

func GenDefaultGroupID() string {
	return GetHashMacAddrs()
}
