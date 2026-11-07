package main

import (
	"net"
	"os"
	"strings"
)

// sdNotify sends state to the service manager's NOTIFY_SOCKET, as systemd's
// sd_notify does. With the variable unset it does nothing, so a unit that is
// not Type=notify runs as before.
func sdNotify(state string) error {
	addr := os.Getenv("NOTIFY_SOCKET")
	if addr == "" {
		return nil
	}
	if strings.HasPrefix(addr, "@") {
		addr = "\x00" + addr[1:]
	}
	conn, err := net.DialUnix("unixgram", nil, &net.UnixAddr{Name: addr, Net: "unixgram"})
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	_, err = conn.Write([]byte(state))
	return err
}
