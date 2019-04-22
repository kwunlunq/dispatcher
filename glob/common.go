package glob

func ErrTopic(topic string) string {
	return topic + "_ERR"
}

func TrimBytes(bytes []byte) string {
	str := string(bytes)
	if len(str) > 200 {
		return str[:200] + " ..."
	}
	return str
}
