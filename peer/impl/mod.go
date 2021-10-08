package impl

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"go.dedis.ch/cs438/peer"
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

const defaultLevel = zerolog.InfoLevel

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
	node := &node{
		conf:         conf,
		done:         make(chan struct{}, 1),
		routingTable: NewConcurrentMap(),
	}
	node.logger = zerolog.New(logout).Level(level).With().
		Timestamp().
		Caller().
		Str("role", "node").
		Str("myaddr", node.getAddr()).Logger()
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
				select {
				case <-n.done:
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

	return n.conf.Socket.Send(nextHop, pkt, timeout)
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
