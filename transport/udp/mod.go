package udp

import (
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"go.dedis.ch/cs438/internal/traffic"
	"go.dedis.ch/cs438/transport"
)

const bufSize = 65000

// NewUDP returns a new udp transport implementation.
func NewUDP() transport.Transport {
	return &UDP{traffic: traffic.NewTraffic()}
}

// UDP implements a transport layer using UDP
//
// - implements transport.Transport
type UDP struct {
	sync.RWMutex

	traffic *traffic.Traffic
}

// CreateSocket implements transport.Transport
func (n *UDP) CreateSocket(address string) (transport.ClosableSocket, error) {
	// n.Lock()
	// if strings.HasSuffix(address, ":0") {
	// 	address = address[:len(address)-2]
	// 	port := atomic.AddUint32(&counter, 1)
	// 	address = fmt.Sprintf("%s:%d", address, port)
	// }
	// n.Unlock()
	conn, err := net.ListenPacket("udp", address)
	if err != nil {
		return nil, fmt.Errorf("failed to listen: %v", err)
	}

	return &Socket{
		UDP:    n,
		myAddr: conn.LocalAddr().String(),

		conn: conn,

		ins:  packets{},
		outs: packets{},
	}, nil
}

// Socket implements a network socket using UDP.
//
// - implements transport.Socket
// - implements transport.ClosableSocket
type Socket struct {
	*UDP
	myAddr string

	conn net.PacketConn

	ins  packets
	outs packets
}

// Close implements transport.Socket. It returns an error if already closed.
func (s *Socket) Close() error {
	return s.conn.Close()
}

// Send implements transport.Socket
func (s *Socket) Send(dest string, pkt transport.Packet, timeout time.Duration) error {
	conn, err := net.Dial("udp", dest)
	if err != nil {
		return fmt.Errorf("failed to dial: %v", err)
	}
	defer conn.Close()

	b, err := pkt.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal: %v", err)
	}

	if timeout > 0 {
		conn.SetWriteDeadline(time.Now().Add(timeout))
	}
	_, err = conn.Write(b)
	if err != nil {
		if errors.Is(err, os.ErrDeadlineExceeded) {
			return transport.TimeoutErr(timeout)
		}
		return fmt.Errorf("failed to write to connnection: %v", err)
	}

	s.outs.add(pkt)
	s.traffic.LogSent(pkt.Header.RelayedBy, dest, pkt)

	return nil
}

// Recv implements transport.Socket. It blocks until a packet is received, or
// the timeout is reached. In the case the timeout is reached, return a
// TimeoutErr.
func (s *Socket) Recv(timeout time.Duration) (transport.Packet, error) {
	if timeout > 0 {
		s.conn.SetReadDeadline(time.Now().Add(timeout))
	}

	p := make([]byte, bufSize)
	n, _, err := s.conn.ReadFrom(p)
	if err != nil {
		if errors.Is(err, os.ErrDeadlineExceeded) {
			return transport.Packet{}, transport.TimeoutErr(timeout)
		}
		return transport.Packet{}, fmt.Errorf("failed to read from: %v", err)
	}

	var pkt transport.Packet
	err = pkt.Unmarshal(p[:n])
	if err != nil {
		return transport.Packet{}, fmt.Errorf("fail to unmarshall: %v", err)
	}

	s.traffic.LogRecv(pkt.Header.RelayedBy, s.myAddr, pkt)
	s.ins.add(pkt)
	return pkt, nil

}

// GetAddress implements transport.Socket. It returns the address assigned. Can
// be useful in the case one provided a :0 address, which makes the system use a
// random free port.
func (s *Socket) GetAddress() string {
	return s.myAddr
}

// GetIns implements transport.Socket
func (s *Socket) GetIns() []transport.Packet {
	return s.ins.getAll()
}

// GetOuts implements transport.Socket
func (s *Socket) GetOuts() []transport.Packet {
	return s.outs.getAll()
}

type packets struct {
	sync.Mutex
	data []transport.Packet
}

func (p *packets) add(pkt transport.Packet) {
	p.Lock()
	defer p.Unlock()

	p.data = append(p.data, pkt.Copy())
}

func (p *packets) getAll() []transport.Packet {
	p.Lock()
	defer p.Unlock()

	res := make([]transport.Packet, len(p.data))

	for i, pkt := range p.data {
		res[i] = pkt.Copy()
	}

	return res
}
