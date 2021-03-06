package glob

import (
	"gitlab.paradise-soft.com.tw/glob/dispatcher/glob/core"
	"net"
	"strings"
)

func GetMacAddrs() (macAddrs []string) {
	netInterfaces, err := net.Interfaces()
	if err != nil {
		core.Logger.Errorf("Failed to get mac address: %v", err)
		return macAddrs
	}

	for _, netInterface := range netInterfaces {
		macAddr := netInterface.HardwareAddr.String()
		if len(macAddr) == 0 {
			continue
		}
		macAddrs = append(macAddrs, macAddr)
	}
	return macAddrs
}

func GetHashMacAddrs() string {
	return HashMd5(strings.Join(GetMacAddrs(), ","))
}

// CorrectUrl by suffix "/" and prefix "http://"
func CorrectUrl(str string) string {
	str = strings.TrimSpace(str)
	if !strings.HasSuffix(str, "/") {
		str += "/"
	}
	if !strings.HasPrefix(str, "http://") && !strings.HasPrefix(str, "https://") {
		str = "http://" + str
	}
	return str
}
