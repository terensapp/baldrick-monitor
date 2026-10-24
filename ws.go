package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// wsConn is a minimal WebSocket client: enough for the board's "baldrick"
// subprotocol (small JSON text frames, client keepalives) and Discord's
// gateway (JSON over TLS).
type wsConn struct {
	c    net.Conn
	br   *bufio.Reader
	wmu  sync.Mutex
	peer string // who closes it, for errors
}

// wsCloseError is a close frame from the other end.
type wsCloseError struct {
	Peer string
	Code int // 0 when the frame carried none
}

func (e *wsCloseError) Error() string {
	if e.Code == 0 {
		return "closed by " + e.Peer
	}
	return fmt.Sprintf("closed by %s (code %d)", e.Peer, e.Code)
}

func wsDial(host string, timeout time.Duration) (*wsConn, error) {
	c, err := dialBoard(context.Background(), "tcp", httpAddr(host), timeout)
	if err != nil {
		return nil, err
	}
	return wsHandshake(c, host, "/ws", "baldrick", "board", timeout)
}

// wsDialURL connects to a ws:// or wss:// URL.
func wsDialURL(ctx context.Context, raw, peer string, timeout time.Duration) (*wsConn, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	addr := u.Host
	if u.Port() == "" {
		addr = net.JoinHostPort(u.Hostname(), map[string]string{"wss": "443", "ws": "80"}[u.Scheme])
	}
	d := &net.Dialer{Timeout: timeout}
	var c net.Conn
	switch u.Scheme {
	case "wss":
		c, err = (&tls.Dialer{NetDialer: d, Config: &tls.Config{ServerName: u.Hostname()}}).DialContext(ctx, "tcp", addr)
	case "ws":
		c, err = d.DialContext(ctx, "tcp", addr)
	default:
		return nil, fmt.Errorf("not a websocket URL: %s", raw)
	}
	if err != nil {
		return nil, err
	}
	return wsHandshake(c, u.Host, u.RequestURI(), "", peer, timeout)
}

func wsHandshake(c net.Conn, host, path, proto, peer string, timeout time.Duration) (*wsConn, error) {
	kb := make([]byte, 16)
	rand.Read(kb)
	key := base64.StdEncoding.EncodeToString(kb)
	c.SetDeadline(time.Now().Add(timeout))
	if proto != "" {
		proto = "Sec-WebSocket-Protocol: " + proto + "\r\n"
	}
	fmt.Fprintf(c, "GET %s HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"+
		"Sec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n%s\r\n", path, host, key, proto)
	br := bufio.NewReader(c)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		c.Close()
		return nil, err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusSwitchingProtocols {
		c.Close()
		return nil, fmt.Errorf("websocket upgrade: HTTP %d", resp.StatusCode)
	}
	h := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	if resp.Header.Get("Sec-WebSocket-Accept") != base64.StdEncoding.EncodeToString(h[:]) {
		c.Close()
		return nil, errors.New("websocket upgrade: bad accept key")
	}
	c.SetDeadline(time.Time{})
	return &wsConn{c: c, br: br, peer: peer}, nil
}

func (w *wsConn) Close() error { return w.c.Close() }

// read returns the next text/binary message, answering pings on the way.
func (w *wsConn) read(timeout time.Duration) ([]byte, error) {
	var msg []byte
	for {
		w.c.SetReadDeadline(time.Now().Add(timeout))
		var h [2]byte
		if _, err := io.ReadFull(w.br, h[:]); err != nil {
			return nil, err
		}
		fin, op := h[0]&0x80 != 0, h[0]&0x0f
		n := uint64(h[1] & 0x7f)
		switch n {
		case 126:
			var e [2]byte
			if _, err := io.ReadFull(w.br, e[:]); err != nil {
				return nil, err
			}
			n = uint64(binary.BigEndian.Uint16(e[:]))
		case 127:
			var e [8]byte
			if _, err := io.ReadFull(w.br, e[:]); err != nil {
				return nil, err
			}
			n = binary.BigEndian.Uint64(e[:])
		}
		if n > 1<<20 {
			return nil, errors.New("websocket frame too large")
		}
		var mask [4]byte
		masked := h[1]&0x80 != 0
		if masked {
			if _, err := io.ReadFull(w.br, mask[:]); err != nil {
				return nil, err
			}
		}
		p := make([]byte, n)
		if _, err := io.ReadFull(w.br, p); err != nil {
			return nil, err
		}
		if masked {
			for i := range p {
				p[i] ^= mask[i%4]
			}
		}
		switch op {
		case 0x8:
			e := &wsCloseError{Peer: w.peer}
			if len(p) >= 2 {
				e.Code = int(binary.BigEndian.Uint16(p))
			}
			return nil, e
		case 0x9:
			w.write(0xA, p)
			continue
		case 0xA:
			continue
		}
		msg = append(msg, p...)
		if fin {
			return msg, nil
		}
	}
}

func (w *wsConn) writeText(p []byte) error { return w.write(0x1, p) }

func (w *wsConn) write(op byte, p []byte) error {
	w.wmu.Lock()
	defer w.wmu.Unlock()
	f := []byte{0x80 | op}
	switch {
	case len(p) < 126:
		f = append(f, 0x80|byte(len(p)))
	case len(p) < 1<<16:
		f = append(f, 0x80|126, byte(len(p)>>8), byte(len(p)))
	default:
		f = append(f, 0x80|127)
		f = binary.BigEndian.AppendUint64(f, uint64(len(p)))
	}
	var mask [4]byte
	rand.Read(mask[:])
	f = append(f, mask[:]...)
	for i, b := range p {
		f = append(f, b^mask[i%4])
	}
	w.c.SetWriteDeadline(time.Now().Add(2 * time.Second))
	_, err := w.c.Write(f)
	return err
}
