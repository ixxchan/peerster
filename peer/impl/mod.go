package impl

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/registry"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
)

// envLogLevel is the name of the environment variable to change the logging
// level.
//
//   NODELOG=trace go test ./...
//   NODELOG=info go test ./...
//
const envLogLevel = "NODELOG"

const defaultLevel = zerolog.Disabled

var level = defaultLevel

func init() {
	switch os.Getenv(envLogLevel) {
	case "error":
		level = zerolog.ErrorLevel
	case "warn":
		level = zerolog.WarnLevel
	case "info":
		level = zerolog.InfoLevel
	case "debug":
		level = zerolog.DebugLevel
	case "trace":
		level = zerolog.TraceLevel
	case "":
		level = defaultLevel
	default:
		level = zerolog.TraceLevel
	}
}

var logout = zerolog.ConsoleWriter{
	Out:        os.Stdout,
	TimeFormat: time.RFC3339,
}

const timeout = time.Second * 1

// NewPeer creates a new peer. You can change the content and location of this
// function but you MUST NOT change its signature and package location.
func NewPeer(conf peer.Configuration) peer.Peer {
	ctx, cancelFunc := context.WithCancel(context.Background())

	node := &node{
		conf:         conf,
		routingTable: NewConcurrentMap(),
		status:       make(map[string]uint),
		rumorsLog:    make(map[string][]types.Rumor),

		ctx:        ctx,
		cancelFunc: cancelFunc,
	}
	node.logger = zerolog.New(logout).Level(level).With().
		Timestamp().
		Caller().
		Str("role", "node").
		Str("myaddr", node.getAddr()).Logger()

	ctrl := NewController(node)
	conf.MessageRegistry.RegisterMessageCallback(types.ChatMessage{}, logging(&node.logger)(ctrl.chatHandler))
	conf.MessageRegistry.RegisterMessageCallback(types.RumorsMessage{}, logging(&node.logger)(ctrl.rumorsHandler))
	conf.MessageRegistry.RegisterMessageCallback(types.StatusMessage{}, logging(&node.logger)(ctrl.statusHandler))
	conf.MessageRegistry.RegisterMessageCallback(types.AckMessage{}, logging(&node.logger)(ctrl.ackHandler))
	conf.MessageRegistry.RegisterMessageCallback(types.EmptyMessage{}, logging(&node.logger)(ctrl.emptyHandler))
	conf.MessageRegistry.RegisterMessageCallback(types.PrivateMessage{}, logging(&node.logger)(ctrl.privateHandler))

	// routingTable[myAddr] = myAddr
	node.AddPeer(conf.Socket.GetAddress())
	return node
}

type neighbors struct {
	sync.RWMutex
	data []string
}

func (ns *neighbors) len() int {
	ns.Lock()
	defer ns.Unlock()

	return len(ns.data)
}

func (ns *neighbors) add(neighbor string) {
	ns.Lock()
	defer ns.Unlock()

	ns.data = append(ns.data, neighbor)
}

func (ns *neighbors) delete(neighbor string) {
	ns.Lock()
	defer ns.Unlock()

	for i, v := range ns.data {
		if v == neighbor {
			ns.data = append(ns.data[:i], ns.data[i+1:]...)
			return
		}
	}
}

func (ns *neighbors) getRandom() string {
	ns.RLock()
	defer ns.RUnlock()

	return ns.data[rand.Intn(len(ns.data))]
}

func (ns *neighbors) has(neighbor string) bool {
	ns.Lock()
	defer ns.Unlock()

	for _, v := range ns.data {
		if v == neighbor {
			return true
		}
	}
	return false
}

// node implements a peer to build a Peerster system
//
// - implements peer.Peer
type node struct {
	conf         peer.Configuration
	routingTable ConcurrentMap

	neighbors neighbors
	// rumor sequence number, equal to the number of rumors this node has created
	//
	// should use atomic operation to get/set
	seq uint32

	// a node’s view on the system. By “view” we mean all the rumors the
	// node has processed so far.
	//
	// map peer -> the sequence number of the last Rumor processed from that peer.
	status map[string]uint
	// TODO: what about store Msg only?
	rumorsLog map[string][]types.Rumor
	// protect status & rumorsLog
	rumorsMu sync.Mutex

	// PacketID -> chan struct{}
	//
	// This is used in Broadcast() and ackHandler().
	// They do not require rumorsMu, and accessing the whole map is not needed, so simply use sync.Map.
	ackCh sync.Map

	ctx        context.Context
	cancelFunc context.CancelFunc

	logger zerolog.Logger
}

// Start implements peer.Service
func (n *node) Start() error {
	if n.conf.HeartbeatInterval > 0 {
		go n.heartbeat()
	}
	if n.conf.AntiEntropyInterval > 0 {
		go n.antyEntroy()
	}
	n.logger.Info().Msgf("starting ...")
	go func() {
		for {
			pkt, err := n.conf.Socket.Recv(time.Second * 1)
			if errors.Is(err, transport.TimeoutErr(0)) {
				continue
			}
			if err != nil {
				select {
				case <-n.ctx.Done():
					return
				default:
					n.logger.Warn().Msgf("error when receive packet: %v", err)
					continue
				}
			}

			pktLogger := n.logger.With().
				Str("source", pkt.Header.Source).
				Str("destination", pkt.Header.Destination).
				Str("packet_id", pkt.Header.PacketID).Logger()
			pktLogger.Info().Msgf("received packet")
			if pkt.Header.Destination != n.getAddr() {
				nextHop := n.routingTable.Get(pkt.Header.Destination)
				if nextHop == "" {
					n.logger.Info().Msgf("unknown relay destination: %v", pkt.Header.Destination)
				} else {
					pktLogger.Info().Msgf("relay packet")
					pkt.Header.RelayedBy = n.getAddr()
					go func() {
						if err := n.conf.Socket.Send(nextHop, pkt, timeout); err != nil {
							pktLogger.Info().Msgf("error when send packet: %v", err)
						}
					}()
				}
			} else {
				go func() {
					if err := n.conf.MessageRegistry.ProcessPacket(pkt); err != nil {
						pktLogger.Info().Msgf("error when process packet: %v", err)
					}
				}()
			}

			select {
			case <-n.ctx.Done():
				return
			default:
			}
		}
	}()

	return nil
}

// Stop implements peer.Service
func (n *node) Stop() error {
	n.cancelFunc()
	return nil
}

// Unicast implements peer.Messaging
func (n *node) Unicast(dest string, msg transport.Message) error {
	dest = strings.TrimSpace(dest)
	nextHop := n.routingTable.Get(dest)
	if nextHop == "" {
		return fmt.Errorf("unknown dest: %v", dest)
	}

	hdr := transport.NewHeader(n.getAddr(), n.getAddr(), dest, 0)
	pkt := transport.Packet{
		Msg:    &msg,
		Header: &hdr,
	}

	return n.conf.Socket.Send(nextHop, pkt, timeout)
}

// Broadcast implements peer.Messaging
func (n *node) Broadcast(msg transport.Message) error {
	// TODO: If error occurs, do we need to decrement?
	atomic.AddUint32(&n.seq, 1)
	rumors := types.RumorsMessage{Rumors: []types.Rumor{
		{
			Origin:   n.getAddr(),
			Sequence: uint(atomic.LoadUint32(&n.seq)),
			Msg:      &msg,
		},
	}}

	// execute the message "locally" first, so that my status is updated
	hdr := transport.NewHeader(n.getAddr(), n.getAddr(), n.getAddr(), 1)
	pkt := transport.Packet{
		Header: &hdr,
		Msg:    n.getMarshalledMsg(&rumors),
	}
	if err := n.conf.MessageRegistry.ProcessPacket(pkt); err != nil {
		return fmt.Errorf("failed to process rumor locally: %v", err)
	}

	// sends the rumor to a random neighbor.
	if n.neighbors.len() == 0 {
		n.logger.Warn().Msgf("has none neighbors")
		return nil
	}
	neighbor := n.neighbors.getRandom()
	hdr = transport.NewHeader(n.getAddr(), n.getAddr(), neighbor, 1)
	pkt.Header = &hdr

	ackCh := make(chan struct{}, 1)
	n.ackCh.Store(pkt.Header.PacketID, ackCh)

	if err := n.conf.Socket.Send(neighbor, pkt, timeout); err != nil {
		return fmt.Errorf("failed to send rumor: %v", err)
	}
	n.logger.Debug().Str("packet_id", pkt.Header.PacketID).Msgf("rumor is sent to %v", neighbor)

	// wait for ack. If no ack, send to another neighbor
	go func() {
		for acked := false; !acked; {
			n.logger.Debug().Msgf("broadcast wait for ack %v, neighbor %v", pkt.Header.PacketID, pkt.Header.Destination)

			if n.conf.AckTimeout == 0 {
				<-ackCh
				acked = true
			} else {
				select {
				case <-ackCh:
					acked = true
				case <-time.After(n.conf.AckTimeout):
					// TODO: in this case, how to gc (delete map entry)?
					// send the RumorMessage to another random neighbor
					if n.neighbors.len() == 1 {
						n.logger.Warn().Msgf("neighbor did not ack, but has only 1 neighbor")
						return
					}
					newNeighbor := n.neighbors.getRandom()
					for newNeighbor == neighbor {
						newNeighbor = n.neighbors.getRandom()
					}
					neighbor = newNeighbor
					hdr = transport.NewHeader(n.getAddr(), n.getAddr(), neighbor, 1)
					pkt.Header = &hdr

					ackCh := make(chan struct{}, 1)
					n.ackCh.Store(pkt.Header.PacketID, ackCh)

					if err := n.conf.Socket.Send(neighbor, pkt, timeout); err != nil {
						n.logger.Error().Msgf("failed to send rumor: %v", err)
					}
				}
			}
		}
	}()

	return nil
}

// AddPeer implements peer.Service
func (n *node) AddPeer(addr ...string) {
	for _, addr := range addr {
		n.SetRoutingEntry(addr, addr)
	}
}

// GetRoutingTable implements peer.Service
func (n *node) GetRoutingTable() peer.RoutingTable {
	return n.routingTable.GetMap()
}

// SetRoutingEntry implements peer.Service
func (n *node) SetRoutingEntry(origin, relayAddr string) {
	origin = strings.TrimSpace(origin)
	relayAddr = strings.TrimSpace(relayAddr)
	if relayAddr == "" {
		n.routingTable.Delete(origin)
		n.neighbors.delete(origin)
	} else {
		n.routingTable.Set(origin, relayAddr)
		if origin == relayAddr && origin != n.getAddr() {
			n.neighbors.add(origin)
		}
	}
}

func (n *node) getAddr() string {
	return n.conf.Socket.GetAddress()
}

func (n *node) heartbeat() {
	msg := transport.Message{
		Type: types.EmptyMessage{}.Name(),
	}

	for {
		// TODO: cannot call Broadcast?
		err := n.Broadcast(msg)
		if err != nil {
			n.logger.Trace().Msgf("heartbeat failed")
		}

		select {
		case <-n.ctx.Done():
			return
		default:
		}
		time.Sleep(n.conf.HeartbeatInterval)
	}
}

// Convert types.Message (must be pointer) to *transport.Message.
func (n *node) getMarshalledMsg(msg types.Message) *transport.Message {
	logger := n.logger.With().CallerWithSkipFrameCount(3).Logger() // report the caller of this function
	m, err := n.conf.MessageRegistry.MarshalMessage(msg)
	if err != nil {
		logger.Error().Msgf("failed to marshall message: %v", err)
		// TODO: or return nil/error?
		return &transport.Message{
			Type:    msg.Name(),
			Payload: nil,
		}
	}
	return &m
}

func (n *node) antyEntroy() {
	for n.neighbors.len() == 0 {
		time.Sleep(n.conf.AntiEntropyInterval)
	}
	pkt := transport.Packet{}
	for {
		neighbor := n.neighbors.getRandom()
		hdr := transport.NewHeader(n.getAddr(), n.getAddr(), neighbor, 1)
		pkt.Header = &hdr
		n.rumorsMu.Lock()
		pkt.Msg = n.getMarshalledMsg((*types.StatusMessage)(&n.status))
		n.rumorsMu.Unlock()
		if err := n.conf.Socket.Send(neighbor, pkt, timeout); err != nil {
			n.logger.Trace().Msgf("failed to send anti-entropy status: %v", err)
		}

		select {
		case <-n.ctx.Done():
			return
		default:
		}
		time.Sleep(n.conf.AntiEntropyInterval)
	}
}

// logging is a utility function that adds logging in a handler
func logging(logger *zerolog.Logger) func(registry.Exec) registry.Exec {
	return func(next registry.Exec) registry.Exec {
		return func(m types.Message, p transport.Packet) error {
			newlogger := logger.With().CallerWithSkipFrameCount(2).Logger()
			newlogger.Info().
				Str("source", p.Header.Source).
				Str("packet_id", p.Header.PacketID).
				Str("message_type", m.Name()).
				Msgf("process message: %v", m.String())
			return next(m, p)
		}
	}
}
