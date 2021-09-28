package impl

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/rs/zerolog"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
)

const defaultLevel = zerolog.NoLevel

var logout = zerolog.ConsoleWriter{
	Out:        os.Stdout,
	TimeFormat: time.RFC3339,
}

var Logger = zerolog.New(logout).Level(defaultLevel).
	With().Timestamp().Logger().
	With().Caller().Logger()

// NewPeer creates a new peer. You can change the content and location of this
// function but you MUST NOT change its signature and package location.
func NewPeer(conf peer.Configuration) peer.Peer {
	node := &node{
		conf:         conf,
		done:         make(chan struct{}, 1),
		routingTable: NewConcurrentMap(),
	}

	// routingTable[myAddr] = myAddr
	node.AddPeer(conf.Socket.GetAddress())
	return node
}

// node implements a peer to build a Peerster system
//
// - implements peer.Peer
type node struct {
	conf         peer.Configuration
	routingTable ConcurrentMap

	done chan struct{}
}

// Start implements peer.Service
func (n *node) Start() error {
	go func() {
		for {
			pkt, err := n.conf.Socket.Recv(time.Second * 1)
			if errors.Is(err, transport.TimeoutErr(0)) {
				continue
			}
			if err != nil {
				Logger.Warn().Msgf("error when receive packet: %v", err)
			}

			Logger.Info().
				Str("source", pkt.Header.Source).
				Str("packet_id", pkt.Header.PacketID).
				Msgf("received packet")

			select {
			case <-n.done:
				return
			default:
			}
		}
	}()

	return nil
}

// Stop implements peer.Service
func (n *node) Stop() error {
	select {
	case n.done <- struct{}{}:
		return nil
	default:
		return fmt.Errorf("already stopped")
	}
}

// Unicast implements peer.Messaging
func (n *node) Unicast(dest string, msg transport.Message) error {
	panic("to be implemented in HW0")
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
	if relayAddr == "" {
		n.routingTable.Delete(origin)
	} else {
		n.routingTable.Set(origin, relayAddr)
	}
}
