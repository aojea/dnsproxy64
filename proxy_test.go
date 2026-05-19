package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/dns/dnsmessage"
)

type mockBackend struct {
	udpConn *net.UDPConn
	tcpLn   net.Listener
	dropUDP atomic.Bool

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func startMockBackend(t *testing.T) *mockBackend {
	t.Helper()

	tcpLn, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen tcp: %v", err)
	}
	port := tcpLn.Addr().(*net.TCPAddr).Port
	udpConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port})
	if err != nil {
		tcpLn.Close()
		t.Fatalf("ListenUDP: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	b := &mockBackend{udpConn: udpConn, tcpLn: tcpLn, ctx: ctx, cancel: cancel}
	b.wg.Add(2)
	go b.udpLoop()
	go b.tcpLoop()
	return b
}

func (b *mockBackend) addr() string {
	return b.udpConn.LocalAddr().String()
}

func (b *mockBackend) close() {
	b.cancel()
	_ = b.udpConn.Close()
	_ = b.tcpLn.Close()
	b.wg.Wait()
}

func (b *mockBackend) udpLoop() {
	defer b.wg.Done()
	buf := make([]byte, maxUDPPacketSize)
	for {
		n, addr, err := b.udpConn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		if b.dropUDP.Load() {
			continue
		}
		resp := make([]byte, n)
		copy(resp, buf[:n])
		if n >= 3 {
			resp[2] |= 0x80
		}
		_, _ = b.udpConn.WriteToUDP(resp, addr)
	}
}

func (b *mockBackend) tcpLoop() {
	defer b.wg.Done()
	for {
		conn, err := b.tcpLn.Accept()
		if err != nil {
			return
		}
		b.wg.Add(1)
		go func(c net.Conn) {
			defer b.wg.Done()
			defer c.Close()
			for {
				var lenBuf [2]byte
				if _, err := io.ReadFull(c, lenBuf[:]); err != nil {
					return
				}
				n := binary.BigEndian.Uint16(lenBuf[:])
				msg := make([]byte, n)
				if _, err := io.ReadFull(c, msg); err != nil {
					return
				}
				if len(msg) >= 3 {
					msg[2] |= 0x80
				}
				var outLen [2]byte
				binary.BigEndian.PutUint16(outLen[:], uint16(len(msg)))
				if _, err := c.Write(outLen[:]); err != nil {
					return
				}
				if _, err := c.Write(msg); err != nil {
					return
				}
			}
		}(conn)
	}
}

func startTestProxy(t *testing.T, backendAddr string) *proxy {
	t.Helper()
	p := newProxy("[::1]:0", backendAddr)
	p.udpStateTTL = 300 * time.Millisecond
	p.gcInterval = 100 * time.Millisecond
	if err := p.start(); err != nil {
		t.Fatalf("start proxy: %v", err)
	}
	t.Cleanup(p.shutdown)
	return p
}

func buildDNSQuery(t *testing.T, id uint16) []byte {
	t.Helper()
	b := dnsmessage.NewBuilder(nil, dnsmessage.Header{ID: id, RecursionDesired: true})
	if err := b.StartQuestions(); err != nil {
		t.Fatalf("StartQuestions: %v", err)
	}
	if err := b.Question(dnsmessage.Question{
		Name:  dnsmessage.MustNewName("example.com."),
		Type:  dnsmessage.TypeA,
		Class: dnsmessage.ClassINET,
	}); err != nil {
		t.Fatalf("Question: %v", err)
	}
	msg, err := b.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	return msg
}

func parseDNSHeader(t *testing.T, msg []byte) dnsmessage.Header {
	t.Helper()
	var parser dnsmessage.Parser
	h, err := parser.Start(msg)
	if err != nil {
		t.Fatalf("parse header: %v", err)
	}
	return h
}

func TestProxyUDPEndToEndConcurrent(t *testing.T) {
	backend := startMockBackend(t)
	defer backend.close()
	p := startTestProxy(t, backend.addr())

	const requests = 200
	errCh := make(chan error, requests)
	var wg sync.WaitGroup

	for i := 0; i < requests; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			client, err := net.DialUDP("udp6", nil, p.udpConn.LocalAddr().(*net.UDPAddr))
			if err != nil {
				errCh <- err
				return
			}
			defer client.Close()

			id := uint16(i + 1000)
			req := buildDNSQuery(t, id)
			if err := client.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
				errCh <- err
				return
			}
			if _, err := client.Write(req); err != nil {
				errCh <- err
				return
			}
			buf := make([]byte, 1500)
			n, _, err := client.ReadFromUDP(buf)
			if err != nil {
				errCh <- err
				return
			}
			h := parseDNSHeader(t, buf[:n])
			if h.ID != id {
				errCh <- fmt.Errorf("id mismatch got %d want %d", h.ID, id)
				return
			}
			if !h.Response {
				errCh <- fmt.Errorf("response bit not set")
			}
		}(i)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent udp request failed: %v", err)
		}
	}
}

func TestProxyTCPEndToEnd(t *testing.T) {
	backend := startMockBackend(t)
	defer backend.close()
	p := startTestProxy(t, backend.addr())

	conn, err := net.Dial("tcp6", p.tcpListener.Addr().String())
	if err != nil {
		t.Fatalf("Dial tcp6: %v", err)
	}
	defer conn.Close()

	req := buildDNSQuery(t, 4242)
	var lenBuf [2]byte
	binary.BigEndian.PutUint16(lenBuf[:], uint16(len(req)))
	if _, err := conn.Write(lenBuf[:]); err != nil {
		t.Fatalf("write length: %v", err)
	}
	if _, err := conn.Write(req); err != nil {
		t.Fatalf("write request: %v", err)
	}

	if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
		t.Fatalf("read length: %v", err)
	}
	respLen := binary.BigEndian.Uint16(lenBuf[:])
	resp := make([]byte, respLen)
	if _, err := io.ReadFull(conn, resp); err != nil {
		t.Fatalf("read response: %v", err)
	}
	h := parseDNSHeader(t, resp)
	if h.ID != 4242 {
		t.Fatalf("unexpected response id: %d", h.ID)
	}
	if !h.Response {
		t.Fatalf("response bit is not set")
	}
}

func TestUDPStateGarbageCollection(t *testing.T) {
	backend := startMockBackend(t)
	backend.dropUDP.Store(true)
	defer backend.close()
	p := startTestProxy(t, backend.addr())

	client, err := net.DialUDP("udp6", nil, p.udpConn.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatalf("DialUDP: %v", err)
	}
	defer client.Close()

	req := buildDNSQuery(t, 77)
	if _, err := client.Write(req); err != nil {
		t.Fatalf("write udp query: %v", err)
	}

	time.Sleep(2 * p.udpStateTTL)

	p.stateMu.RLock()
	stateLen := len(p.stateByNewID)
	p.stateMu.RUnlock()
	if stateLen != 0 {
		t.Fatalf("expected empty udp state map after gc, got %d", stateLen)
	}
}
