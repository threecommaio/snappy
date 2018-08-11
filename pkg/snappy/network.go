package snappy

import (
	"errors"
	"net"
)

var errMissingLocalIP = errors.New("could not find a local ip")

// GetLocalIP returns the first non-loopback device
func GetLocalIP() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	for _, address := range addrs {
		// check the address type and if it is not a loopback the display it
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String(), nil
			}
		}
	}
	return "", errMissingLocalIP
}
