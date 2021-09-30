package impl

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
)

const TIMEOUT = time.Second * 1

// NewPeer creates a new peer. You can change the content and location of this
// function but you MUST NOT change its signature and package location.
func NewPeer(conf peer.Configuration) peer.Peer {
	node := &node{
		conf:         conf,
		done:         make(chan struct{}, 1),
		routingTable: NewConcurrentMap(),
	}
	node.logger = Logger.With().Str("node", node.getAddr()).Logger()

	conf.MessageRegistry.RegisterMessageCallback(
		types.ChatMessage{},
		func(m types.Message, p transport.Packet) error {
			node.logger.Info().
				Str("source", p.Header.Source).
				Str("packet_id", p.Header.PacketID).
				Msgf("received chat message: %v", m.String())
			return nil
		})

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

	logger zerolog.Logger
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
				n.logger.Warn().Msgf("error when receive packet: %v", err)
			}

			n.logger.Info().
				Str("source", pkt.Header.Source).
				Str("destination", pkt.Header.Destination).
				Str("packet_id", pkt.Header.PacketID).
				Msgf("received packet")
			if pkt.Header.Destination != n.getAddr() {
				nextHop := n.routingTable.Get(pkt.Header.Destination)
				if nextHop == "" {
					n.logger.Info().Msgf("unknown relay destination: %v", pkt.Header.Destination)
				} else {
					n.logger.Info().
						Str("source", pkt.Header.Source).
						Str("destination", pkt.Header.Destination).
						Str("packet_id", pkt.Header.PacketID).
						Msgf("relay packet")
					pkt.Header.RelayedBy = n.getAddr()
					n.conf.Socket.Send(nextHop, pkt, TIMEOUT)
				}
			} else {
				n.conf.MessageRegistry.ProcessPacket(pkt)
			}

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

	return n.conf.Socket.Send(nextHop, pkt, TIMEOUT)
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
	} else {
		n.routingTable.Set(origin, relayAddr)
	}
}

func (n *node) getAddr() string {
	return n.conf.Socket.GetAddress()
}
