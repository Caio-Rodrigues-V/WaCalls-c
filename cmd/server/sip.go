package main

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type SIPServer struct {
	addr string
	log  *slog.Logger
	srv  *server

	mu          sync.Mutex
	activeRTPs  map[string]*RTPBridge
}

type RTPBridge struct {
	conn       *net.UDPConn
	remoteAddr *net.UDPAddr
	stop       chan struct{}
}

func startSIPServer(ctx context.Context, addr string, srv *server, log *slog.Logger) {
	defer func() {
		if r := recover(); r != nil {
			log.Warn("SIP Server panic recovered", "err", r)
		}
	}()
	sip := &SIPServer{
		addr:       addr,
		log:        log,
		srv:        srv,
		activeRTPs: make(map[string]*RTPBridge),
	}
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
			respStr := s.handleSIPMessage(reqStr, remoteAddr.String())
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
		respStr := s.handleSIPMessage(reqStr, conn.RemoteAddr().String())
		if respStr != "" {
			_, _ = conn.Write([]byte(respStr))
		}
	}
}

func (s *SIPServer) handleSIPMessage(msg string, remoteHost string) string {
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

	buildResponse := func(code int, text string, body string) string {
		resp := fmt.Sprintf("SIP/2.0 %d %s\r\nVia: %s\r\nFrom: %s\r\nTo: %s;tag=wacalls123\r\nCall-ID: %s\r\nCSeq: %s\r\nServer: WaCalls-SIP/1.0\r\nContent-Type: application/sdp\r\nContent-Length: %d\r\n\r\n%s",
			code, text, via, from, to, callID, cseq, len(body), body)
		return resp
	}

	if strings.HasPrefix(firstLine, "OPTIONS") {
		return fmt.Sprintf("SIP/2.0 200 OK\r\nVia: %s\r\nFrom: %s\r\nTo: %s;tag=wacalls123\r\nCall-ID: %s\r\nCSeq: %s\r\nAllow: INVITE, ACK, CANCEL, OPTIONS, BYE\r\nContent-Length: 0\r\n\r\n", via, from, to, callID, cseq)
	} else if strings.HasPrefix(firstLine, "INVITE") {
		remoteIP, remotePort := parseSDPAudio(msg)
		if remoteIP == "" {
			host, _, _ := net.SplitHostPort(remoteHost)
			remoteIP = host
		}
		localRTPPort := 10000 + (time.Now().Nanosecond() % 20000)
		sdpAnswer := fmt.Sprintf("v=0\r\no=WaCalls 123456 123456 IN IP4 %s\r\ns=WaCalls Audio\r\nc=IN IP4 %s\r\nt=0 0\r\nm=audio %d RTP/AVP 0 101\r\na=rtpmap:0 PCMU/8000\r\na=rtpmap:101 telephone-event/8000\r\na=sendrecv\r\n",
			"0.0.0.0", "0.0.0.0", localRTPPort)

		s.startRTPBridge(callID, remoteIP, remotePort, localRTPPort)

		return buildResponse(200, "OK", sdpAnswer)
	} else if strings.HasPrefix(firstLine, "REGISTER") {
		return fmt.Sprintf("SIP/2.0 200 OK\r\nVia: %s\r\nFrom: %s\r\nTo: %s;tag=wacalls123\r\nCall-ID: %s\r\nCSeq: %s\r\nContent-Length: 0\r\n\r\n", via, from, to, callID, cseq)
	} else if strings.HasPrefix(firstLine, "CANCEL") || strings.HasPrefix(firstLine, "BYE") {
		s.stopRTPBridge(callID)
		return fmt.Sprintf("SIP/2.0 200 OK\r\nVia: %s\r\nFrom: %s\r\nTo: %s;tag=wacalls123\r\nCall-ID: %s\r\nCSeq: %s\r\nContent-Length: 0\r\n\r\n", via, from, to, callID, cseq)
	}
	return fmt.Sprintf("SIP/2.0 200 OK\r\nVia: %s\r\nFrom: %s\r\nTo: %s;tag=wacalls123\r\nCall-ID: %s\r\nCSeq: %s\r\nContent-Length: 0\r\n\r\n", via, from, to, callID, cseq)
}

func parseSDPAudio(msg string) (string, int) {
	ip := ""
	port := 0
	lines := strings.Split(msg, "\n")
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if strings.HasPrefix(l, "c=IN IP4 ") {
			ip = strings.TrimSpace(l[9:])
		} else if strings.HasPrefix(l, "m=audio ") {
			parts := strings.Split(l, " ")
			if len(parts) >= 2 {
				port, _ = strconv.Atoi(parts[1])
			}
		}
	}
	return ip, port
}

func (s *SIPServer) startRTPBridge(callID, remoteIP string, remotePort, localPort int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	localAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", localPort))
	if err != nil {
		return
	}
	conn, err := net.ListenUDP("udp", localAddr)
	if err != nil {
		return
	}

	var remoteAddr *net.UDPAddr
	if remoteIP != "" && remotePort > 0 {
		remoteAddr, _ = net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", remoteIP, remotePort))
	}

	bridge := &RTPBridge{
		conn:       conn,
		remoteAddr: remoteAddr,
		stop:       make(chan struct{}),
	}
	s.activeRTPs[callID] = bridge

	go s.runRTPRx(bridge, callID)
}

func (s *SIPServer) stopRTPBridge(callID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if b, ok := s.activeRTPs[callID]; ok {
		close(b.stop)
		b.conn.Close()
		delete(s.activeRTPs, callID)
	}
}

func (s *SIPServer) runRTPRx(b *RTPBridge, callID string) {
	buf := make([]byte, 2048)
	for {
		select {
		case <-b.stop:
			return
		default:
			_ = b.conn.SetReadDeadline(time.Now().Add(1 * time.Second))
			n, raddr, err := b.conn.ReadFromUDP(buf)
			if err != nil {
				continue
			}
			if b.remoteAddr == nil && raddr != nil {
				b.remoteAddr = raddr
			}
			if n > 12 {
				// RTP Payload começa após os 12 bytes de cabeçalho RFC 3550
				payload := buf[12:n]
				pcm16k := decodeMulaw8kToPCM16k(payload)
				s.broadcastPCMToCalls(pcm16k)
			}
		}
	}
}

func (s *SIPServer) broadcastPCMToCalls(pcm []float32) {
	if s == nil || s.srv == nil || s.srv.sessions == nil {
		return
	}
	s.srv.sessions.mu.RLock()
	sessionsCopy := make([]*Session, 0, len(s.srv.sessions.sessions))
	for _, sess := range s.srv.sessions.sessions {
		sessionsCopy = append(sessionsCopy, sess)
	}
	s.srv.sessions.mu.RUnlock()

	for _, sess := range sessionsCopy {
		if sess.reg == nil {
			continue
		}
		sess.reg.mu.Lock()
		for _, ac := range sess.reg.calls {
			if ac.cm != nil {
				ac.cm.FeedCapturedPCM(pcm)
			}
		}
		sess.reg.mu.Unlock()
	}
}

// Decodificação G.711 mu-law 8kHz -> PCM 16kHz Float32 (Linear Interpolation)
func decodeMulaw8kToPCM16k(mulaw []byte) []float32 {
	out := make([]float32, len(mulaw)*2)
	for i, u := range mulaw {
		sampleInt16 := decodeMulawSample(u)
		sampleFloat := float32(sampleInt16) / 32768.0
		out[i*2] = sampleFloat
		out[i*2+1] = sampleFloat
	}
	return out
}

func decodeMulawSample(u byte) int16 {
	u = ^u
	sign := int16(u & 0x80)
	exponent := int16((u >> 4) & 0x07)
	mantissa := int16(u & 0x0F)
	sample := ((mantissa << 3) + 0x84) << exponent
	sample -= 0x84
	if sign != 0 {
		return -sample
	}
	return sample
}
