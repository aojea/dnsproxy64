package main

import (
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"io"
	"log"
	"net"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	defaultListenAddr  = "[::1]:53"
	defaultBackendAddr = "127.0.0.11:12345"
	defaultUDPTTL      = 5 * time.Second
	defaultGCInterval  = 2 * time.Second
)

type udpState struct {
	clientAddr *net.UDPAddr
	originalID uint16
	createdAt  time.Time
}

type proxy struct {
	listenAddr  string
	backendAddr string

	udpConn      *net.UDPConn
	udpBackend   *net.UDPConn
	tcpListener  net.Listener
	udpStateTTL  time.Duration
	gcInterval   time.Duration
	nextID       atomic.Uint32
	stateMu      sync.RWMutex
	stateByNewID map[uint16]udpState

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newProxy(listenAddr, backendAddr string) *proxy {
	return &proxy{
		listenAddr:   listenAddr,
		backendAddr:  backendAddr,
		udpStateTTL:  defaultUDPTTL,
		gcInterval:   defaultGCInterval,
		stateByNewID: map[uint16]udpState{},
	}
}

func (p *proxy) start() error {
	udpListenAddr, err := net.ResolveUDPAddr("udp6", p.listenAddr)
	if err != nil {
		return err
	}
	p.udpConn, err = net.ListenUDP("udp6", udpListenAddr)
	if err != nil {
		return err
	}

	udpBackendAddr, err := net.ResolveUDPAddr("udp4", p.backendAddr)
	if err != nil {
		p.udpConn.Close()
		return err
	}
	p.udpBackend, err = net.DialUDP("udp4", nil, udpBackendAddr)
	if err != nil {
		p.udpConn.Close()
		return err
	}

	p.tcpListener, err = net.Listen("tcp6", p.listenAddr)
	if err != nil {
		p.udpBackend.Close()
		p.udpConn.Close()
		return err
	}

	p.ctx, p.cancel = context.WithCancel(context.Background())

	p.wg.Add(4)
	go p.udpClientReadLoop()
	go p.udpBackendReadLoop()
	go p.udpGC()
	go p.tcpAcceptLoop()

	return nil
}

func (p *proxy) shutdown() {
	if p.cancel != nil {
		p.cancel()
	}
	if p.tcpListener != nil {
		_ = p.tcpListener.Close()
	}
	if p.udpConn != nil {
		_ = p.udpConn.Close()
	}
	if p.udpBackend != nil {
		_ = p.udpBackend.Close()
	}
	p.wg.Wait()
}

func (p *proxy) udpClientReadLoop() {
	defer p.wg.Done()

	buf := make([]byte, 65535)
	for {
		n, clientAddr, err := p.udpConn.ReadFromUDP(buf)
		if err != nil {
			if p.ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			continue
		}
		if n < 2 {
			continue
		}

		msg := make([]byte, n)
		copy(msg, buf[:n])
		originalID := binary.BigEndian.Uint16(msg[:2])
		newID, ok := p.reserveUDPState(clientAddr, originalID)
		if !ok {
			continue
		}
		binary.BigEndian.PutUint16(msg[:2], newID)

		if _, err := p.udpBackend.Write(msg); err != nil {
			p.deleteUDPState(newID)
		}
	}
}

func (p *proxy) udpBackendReadLoop() {
	defer p.wg.Done()

	buf := make([]byte, 65535)
	for {
		n, err := p.udpBackend.Read(buf)
		if err != nil {
			if p.ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			continue
		}
		if n < 2 {
			continue
		}

		msg := make([]byte, n)
		copy(msg, buf[:n])
		newID := binary.BigEndian.Uint16(msg[:2])
		state, ok := p.consumeUDPState(newID)
		if !ok {
			continue
		}
		binary.BigEndian.PutUint16(msg[:2], state.originalID)
		_, _ = p.udpConn.WriteToUDP(msg, state.clientAddr)
	}
}

func (p *proxy) udpGC() {
	defer p.wg.Done()

	t := time.NewTicker(p.gcInterval)
	defer t.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-t.C:
			cutoff := time.Now().Add(-p.udpStateTTL)
			p.stateMu.Lock()
			for id, state := range p.stateByNewID {
				if state.createdAt.Before(cutoff) {
					delete(p.stateByNewID, id)
				}
			}
			p.stateMu.Unlock()
		}
	}
}

func (p *proxy) tcpAcceptLoop() {
	defer p.wg.Done()

	for {
		clientConn, err := p.tcpListener.Accept()
		if err != nil {
			if p.ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			continue
		}
		p.wg.Add(1)
		go p.handleTCP(clientConn)
	}
}

func (p *proxy) handleTCP(clientConn net.Conn) {
	defer p.wg.Done()
	defer clientConn.Close()

	backendConn, err := net.Dial("tcp4", p.backendAddr)
	if err != nil {
		return
	}
	defer backendConn.Close()

	var copyWG sync.WaitGroup
	copyWG.Add(2)

	go func() {
		defer copyWG.Done()
		_, _ = io.Copy(backendConn, clientConn)
		if c, ok := backendConn.(*net.TCPConn); ok {
			_ = c.CloseWrite()
		}
	}()

	go func() {
		defer copyWG.Done()
		_, _ = io.Copy(clientConn, backendConn)
		if c, ok := clientConn.(*net.TCPConn); ok {
			_ = c.CloseWrite()
		}
	}()

	copyWG.Wait()
}

func (p *proxy) reserveUDPState(clientAddr *net.UDPAddr, originalID uint16) (uint16, bool) {
	now := time.Now()
	for i := 0; i <= 65535; i++ {
		newID := uint16(p.nextID.Add(1))
		p.stateMu.Lock()
		if _, exists := p.stateByNewID[newID]; !exists {
			p.stateByNewID[newID] = udpState{
				clientAddr: &net.UDPAddr{IP: append(net.IP(nil), clientAddr.IP...), Port: clientAddr.Port, Zone: clientAddr.Zone},
				originalID: originalID,
				createdAt:  now,
			}
			p.stateMu.Unlock()
			return newID, true
		}
		p.stateMu.Unlock()
	}
	return 0, false
}

func (p *proxy) consumeUDPState(newID uint16) (udpState, bool) {
	p.stateMu.Lock()
	defer p.stateMu.Unlock()
	state, ok := p.stateByNewID[newID]
	if ok {
		delete(p.stateByNewID, newID)
	}
	return state, ok
}

func (p *proxy) deleteUDPState(newID uint16) {
	p.stateMu.Lock()
	delete(p.stateByNewID, newID)
	p.stateMu.Unlock()
}

func main() {
	listenAddr := flag.String("listen", defaultListenAddr, "IPv6 listen address")
	backendAddr := flag.String("backend", defaultBackendAddr, "IPv4 backend DNS address")
	flag.Parse()

	p := newProxy(*listenAddr, *backendAddr)
	if err := p.start(); err != nil {
		log.Fatalf("failed to start proxy: %v", err)
	}
	log.Printf("dnsproxy64 listening on %s and forwarding to %s", *listenAddr, *backendAddr)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	p.shutdown()
}
