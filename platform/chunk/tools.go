package chunk

import (
	"net"

	"overlord/pkg/log"
)

// ValidateIPAddress check if given hostname is a valid ipaddress
// and try to resolve if not.
// Return original host if resolving failed
func ValidateIPAddress(hostname string) string {
	ip := net.ParseIP(hostname)
	if ip != nil {
		return ip.String()
	}
	addr, err := net.LookupIP(hostname)
	if err != nil {
		log.Warnf("error resolving hostname %s: %+v", hostname, err)
		return hostname
	}
	if len(addr) == 0 {
		log.Warnf("hostname %s could not be resolved", hostname)
		return hostname
	}
	return addr[0].String()
}
