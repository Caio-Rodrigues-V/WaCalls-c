package main

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"time"
)

type SIPServer struct {
	addr string
	log  *slog.Logger
	srv  *server
}

func startSIPServer(ctx context.Context, addr string, srv *server, log *slog.Logger) {
	sip := &SIPServer{addr: addr, log: log, srv: srv}
	go sip.listenUDP(ctx)
	go sip.listenTCP(ctx)
}

func (s *SIPServer) listenUDP(ctx context.Context) {
	conn, err := net.ListenPacket("udp", s.addr)
	if err != nil {
		s.log.Warn("SIP UDP listen warning", "addr", s.addr, "err", err)
		return
	}
	defer conn.Close()
	s.log.Info("SIP UDP server listening", "addr", s.addr)

	buf := make([]byte, 4096)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
			n, remoteAddr, err := conn.ReadFrom(buf)
			if err != nil {
				continue
			}
			reqStr := string(buf[:n])
			respStr := s.handleSIPMessage(reqStr)
			if respStr != "" {
				_, _ = conn.WriteTo([]byte(respStr), remoteAddr)
			}
		}
	}
}

func (s *SIPServer) listenTCP(ctx context.Context) {
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		s.log.Warn("SIP TCP listen warning", "addr", s.addr, "err", err)
		return
	}
	defer listener.Close()
	s.log.Info("SIP TCP server listening", "addr", s.addr)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			conn, err := listener.Accept()
			if err != nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			go s.handleTCPConn(conn)
		}
	}
}

func (s *SIPServer) handleTCPConn(conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	buf := make([]byte, 4096)
	for {
		_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		n, err := r.Read(buf)
		if err != nil || n == 0 {
			break
		}
		reqStr := string(buf[:n])
		respStr := s.handleSIPMessage(reqStr)
		if respStr != "" {
			_, _ = conn.Write([]byte(respStr))
		}
	}
}

func (s *SIPServer) handleSIPMessage(msg string) string {
	lines := strings.Split(msg, "\r\n")
	if len(lines) == 0 || lines[0] == "" {
		lines = strings.Split(msg, "\n")
	}
	if len(lines) == 0 {
		return ""
	}
	firstLine := lines[0]
	via := ""
	from := ""
	to := ""
	callID := ""
	cseq := ""

	for _, line := range lines {
		l := strings.TrimSpace(line)
		low := strings.ToLower(l)
		if strings.HasPrefix(low, "via:") {
			via = strings.TrimSpace(l[4:])
		} else if strings.HasPrefix(low, "from:") {
			from = strings.TrimSpace(l[5:])
		} else if strings.HasPrefix(low, "to:") {
			to = strings.TrimSpace(l[3:])
		} else if strings.HasPrefix(low, "call-id:") {
			callID = strings.TrimSpace(l[8:])
		} else if strings.HasPrefix(low, "cseq:") {
			cseq = strings.TrimSpace(l[5:])
		}
	}

	buildResponse := func(code int, text string) string {
		return fmt.Sprintf("SIP/2.0 %d %s\r\nVia: %s\r\nFrom: %s\r\nTo: %s;tag=wacalls123\r\nCall-ID: %s\r\nCSeq: %s\r\nServer: WaCalls-SIP/1.0\r\nContent-Length: 0\r\n\r\n",
			code, text, via, from, to, callID, cseq)
	}

	if strings.HasPrefix(firstLine, "OPTIONS") {
		return buildResponse(200, "OK")
	} else if strings.HasPrefix(firstLine, "INVITE") {
		return buildResponse(200, "OK")
	} else if strings.HasPrefix(firstLine, "REGISTER") {
		return buildResponse(200, "OK")
	} else if strings.HasPrefix(firstLine, "CANCEL") || strings.HasPrefix(firstLine, "BYE") {
		return buildResponse(200, "OK")
	}
	return buildResponse(200, "OK")
}
