package impl

import (
	"fmt"
	"math/rand"
	"regexp"
	"runtime/debug"

	"github.com/rs/zerolog"
	"go.dedis.ch/cs438/registry"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
)

type controller struct {
	node *node
}

func NewController(node *node) controller {
	return controller{node: node}
}

func (c *controller) chatHandler(m types.Message, p transport.Packet) error {
	return nil
}

// For local rumors, process them synchronously to ensure no error occurs.
// For rumors from peers, process them parallely and ignore errors.
func (c *controller) rumorsHandler(m types.Message, p transport.Packet) error {
	c.node.rumorsMu.Lock()

	rumors := m.(*types.RumorsMessage)
	newRumor := false

	routingUpdated := make(map[string]bool)
	// Process Rumors
	for _, rumor := range rumors.Rumors {
		if rumor.Sequence != c.node.status[rumor.Origin]+1 {
			// rumor is not expected, ignore it
			continue
		}
		newRumor = true

		if rumor.Origin != c.node.getAddr() && !routingUpdated[rumor.Origin] && !c.node.neighbors.has(rumor.Origin) {
			c.node.SetRoutingEntry(rumor.Origin, p.Header.RelayedBy)
			routingUpdated[rumor.Origin] = true
		}

		newPkt := transport.Packet{
			Header: p.Header,
			Msg:    rumor.Msg,
		}

		c.node.logger.Trace().Msgf("processing rumor %v", rumor)
		if p.Header.Source == c.node.getAddr() {
			if err := c.node.conf.MessageRegistry.ProcessPacket(newPkt); err != nil {
				c.node.rumorsMu.Unlock()
				return err
			}
		} else {
			// Catch up can be faster if process sequentially
			// go func() {
			if err := c.node.conf.MessageRegistry.ProcessPacket(newPkt); err != nil {
				c.node.logger.Error().Msgf("failed to process packet: %v", err)
			}
			// }()
		}

		c.node.status[rumor.Origin]++
		c.node.rumorsLog[rumor.Origin] = append(c.node.rumorsLog[rumor.Origin], rumor)
	}

	if p.Header.Source == c.node.getAddr() {
		c.node.rumorsMu.Unlock()
		return nil
	}

	// Send back an AckMessage to the source
	hdr := transport.NewHeader(c.node.getAddr(), c.node.getAddr(), p.Header.Source, 1)
	ackMsg := types.AckMessage{
		AckedPacketID: p.Header.PacketID,
		Status:        c.node.status,
	}
	ackPkt := transport.Packet{
		Header: &hdr,
		Msg:    c.node.getMarshalledMsg(&ackMsg),
	}
	c.node.rumorsMu.Unlock()

	if err := c.node.conf.Socket.Send(p.Header.Source, ackPkt, SocketTimeout); err != nil {
		return fmt.Errorf("failed to send rumor ack: %v", err)
	}

	// Send the RumorMessage to another random neighbor in the case where one of the Rumor data in the packet is new
	if newRumor {
		if c.node.neighbors.len() == 0 || c.node.neighbors.hasOnly(p.Header.Source) {
			return nil
		}
		neighbor := c.node.neighbors.getNewRandom(p.Header.Source)

		hdr := transport.NewHeader(c.node.getAddr(), c.node.getAddr(), neighbor, 1)
		p.Header = &hdr
		if err := c.node.conf.Socket.Send(neighbor, p, SocketTimeout); err != nil {
			return fmt.Errorf("failed to send rumor: %v", err)
		}
	}

	return nil
}

func (c *controller) statusHandler(m types.Message, p transport.Packet) error {
	c.node.rumorsMu.Lock()
	defer c.node.rumorsMu.Unlock()

	remoteStatus := *m.(*types.StatusMessage)
	remoteHasNew := false
	var remoteMissing []types.Rumor

	// c.node.logger.Trace().Msgf("status %v, my status %v", remoteStatus, c.node.status)

	remoteCnt := 0
	for origin, localSeq := range c.node.status {
		remoteSeq, ok := remoteStatus[origin]
		if ok {
			remoteCnt++
		}
		if remoteSeq < localSeq {
			remoteMissing = append(remoteMissing, c.node.rumorsLog[origin][remoteSeq:]...)
		} else if remoteSeq > localSeq {
			remoteHasNew = true
		}
	}
	if len(remoteStatus) > remoteCnt {
		remoteHasNew = true
	}

	if remoteHasNew {
		hdr := transport.NewHeader(c.node.getAddr(), c.node.getAddr(), p.Header.Source, 0)
		pkt := transport.Packet{
			Header: &hdr,
			Msg:    c.node.getMarshalledMsg((*types.StatusMessage)(&c.node.status)),
		}
		if err := c.node.conf.Socket.Send(p.Header.Source, pkt, SocketTimeout); err != nil {
			c.node.logger.Trace().Msgf("failed to send status: %v", err)
		}
	}

	if remoteMissing != nil {
		hdr := transport.NewHeader(c.node.getAddr(), c.node.getAddr(), p.Header.Source, 0)
		pkt := transport.Packet{
			Header: &hdr,
			Msg:    c.node.getMarshalledMsg(&types.RumorsMessage{Rumors: remoteMissing}),
		}
		if err := c.node.conf.Socket.Send(p.Header.Source, pkt, SocketTimeout); err != nil {
			c.node.logger.Trace().Msgf("failed to send rumors: %v", err)
		}
		// Do not wait for the possible ack message
	}

	if !remoteHasNew && remoteMissing == nil {
		if c.node.neighbors.len() == 0 || c.node.neighbors.hasOnly(p.Header.Source) {
			// do not has new neighbors, skip ContinueMongering
			return nil
		}

		if rand.Float64() < c.node.conf.ContinueMongering {
			// send a status message to a random neighbor
			neighbor := c.node.neighbors.getNewRandom(p.Header.Source)

			hdr := transport.NewHeader(c.node.getAddr(), c.node.getAddr(), neighbor, 0)
			pkt := transport.Packet{
				Header: &hdr,
				Msg:    c.node.getMarshalledMsg((*types.StatusMessage)(&c.node.status)),
			}

			if err := c.node.conf.Socket.Send(neighbor, pkt, SocketTimeout); err != nil {
				c.node.logger.Warn().Msgf("failed to send status: %v", err)
			}
		}
	}

	return nil
}

func (c *controller) ackHandler(m types.Message, p transport.Packet) error {
	ack := m.(*types.AckMessage)
	value, ok := c.node.ackCh.Load(ack.AckedPacketID)
	// The node may not be waiting for ack, when statusHandler sends missing rumor, or it had waited for AckTimeout.
	if ok {
		// Each packet should only be acked once
		ackCh := value.(chan struct{})
		select {
		case ackCh <- struct{}{}:
			close(ackCh)
			c.node.ackCh.Delete(ack.AckedPacketID)
		default:
			c.node.logger.Warn().Str("packet_id", ack.AckedPacketID).Msgf("blocking ack")
		}
	}

	newPkt := transport.Packet{
		Header: p.Header,
		Msg:    c.node.getMarshalledMsg(&ack.Status),
	}
	if err := c.node.conf.MessageRegistry.ProcessPacket(newPkt); err != nil {
		return fmt.Errorf("failed to process status: %v", err)
	}
	return nil
}

func (c *controller) emptyHandler(m types.Message, p transport.Packet) error {
	return nil
}

func (c *controller) privateHandler(m types.Message, p transport.Packet) error {
	pm := m.(*types.PrivateMessage)
	if _, ok := pm.Recipients[c.node.getAddr()]; ok {
		pkt := transport.Packet{
			Header: p.Header,
			Msg:    pm.Msg,
		}
		if err := c.node.conf.MessageRegistry.ProcessPacket(pkt); err != nil {
			return fmt.Errorf("failed to process private message: %v", err)
		}
	}
	return nil
}

func (c *controller) dataRequestHandler(m types.Message, p transport.Packet) error {
	dReq := m.(*types.DataRequestMessage)
	reply := &types.DataReplyMessage{
		RequestID: dReq.RequestID,
		Key:       dReq.Key,
		Value:     c.node.conf.Storage.GetDataBlobStore().Get(dReq.Key),
	}
	msg := c.node.getMarshalledMsg(reply)
	hdr := transport.NewHeader(c.node.getAddr(), c.node.getAddr(), p.Header.Source, 0)
	pkt := transport.Packet{
		Header: &hdr,
		Msg:    msg,
	}
	// The reply message must be sent back using the routing table.
	nextHop := c.node.routingTable.Get(p.Header.Source)
	if err := c.node.conf.Socket.Send(nextHop, pkt, SocketTimeout); err != nil {
		c.node.logger.Warn().
			Str("request_id", dReq.RequestID).
			Str("hash", dReq.Key).
			Str("next_hop", nextHop).
			Msgf("failed to send datareply: %v", err)
		return fmt.Errorf("failed to send datareply: %v", err)
	}
	c.node.logger.Trace().
		Str("request_id", dReq.RequestID).
		Str("hash", dReq.Key).
		Msgf("datareply sent")
	return nil
}

func (c *controller) dataReplyHandler(m types.Message, p transport.Packet) error {
	dRep := m.(*types.DataReplyMessage)
	value, ok := c.node.dataReplyCh.Load(dRep.RequestID)
	if !ok {
		c.node.logger.Trace().Str("request_id", dRep.RequestID).Msgf("not waiting for reply")
		return nil
	}

	dataReplyCh := value.(struct {
		ch  chan []byte
		key string
	})

	if dataReplyCh.key != dRep.Key {
		c.node.logger.Warn().
			Str("request_id", dRep.RequestID).
			Str("reply_key", dRep.Key).
			Str("request_key", dataReplyCh.key).
			Msgf("key mismatch")
		dataReplyCh.ch <- nil
		return nil
	}
	c.node.logger.Trace().
		Str("request_id", dRep.RequestID).
		Str("source", p.Header.Source).
		Str("hash", dRep.Key).
		Msgf("datareply received")

	select {
	case dataReplyCh.ch <- dRep.Value:
	default:
		c.node.logger.Warn().Str("request_id", dRep.RequestID).Msgf("blocking reply")
	}
	return nil
}

func (c *controller) searchRequestHandler(m types.Message, p transport.Packet) error {
	sReq := m.(*types.SearchRequestMessage)

	// Forward the search if the budget permits.
	if sReq.Budget > 1 {
		go func() {
			for neighbor, bgt := range c.node.divideBudget(sReq.Budget-1, p.Header.Source) {
				newReq := sReq
				newReq.Budget = bgt
				hdr := transport.NewHeader(c.node.getAddr(), c.node.getAddr(), neighbor, 0)
				pkt := transport.Packet{
					Header: &hdr,
					Msg:    c.node.getMarshalledMsg(newReq),
				}
				if err := c.node.conf.Socket.Send(neighbor, pkt, SocketTimeout); err != nil {
					c.node.logger.Error().
						Str("request_id", sReq.RequestID).
						Msgf("failed to forward search request: %v", err)
				}
				c.node.logger.Trace().
					Str("request_id", sReq.RequestID).
					Msgf("forwarded search request to %v", neighbor)
			}
		}()
	}

	if _, ok := c.node.handledSearchReq.Load(sReq.RequestID); ok {
		c.node.logger.Trace().Str("request_id", sReq.RequestID).Msgf("duplicate search request")
		return fmt.Errorf("duplicate search request")
	}
	c.node.handledSearchReq.Store(sReq.RequestID, nil)

	sRep := &types.SearchReplyMessage{
		RequestID: sReq.RequestID,
		Responses: c.node.searchLocal(regexp.MustCompile(sReq.Pattern), false),
	}
	msg := c.node.getMarshalledMsg(sRep)

	// The reply must be directly sent to the packet's source (it can be the
	// peer that originated the search request, or a peer that forwarded a search request)
	// without using the routing table.
	// The Destination field of the packet’s header must be
	// set to the searchMessage.Origin.
	hdr := transport.NewHeader(c.node.getAddr(), c.node.getAddr(), sReq.Origin, 0)
	pkt := transport.Packet{
		Header: &hdr,
		Msg:    msg,
	}
	if err := c.node.conf.Socket.Send(p.Header.Source, pkt, SocketTimeout); err != nil {
		c.node.logger.Error().Msgf("failed to send search reply: %v", err)
		return fmt.Errorf("failed to send search reply: %v", err)
	}
	c.node.logger.Trace().
		Str("request_id", sReq.RequestID).
		Str("destination", hdr.Destination).
		Msgf("search reply sent")
	return nil
}

func (c *controller) searchReplyHandler(m types.Message, p transport.Packet) error {
	sRep := m.(*types.SearchReplyMessage)
	c.node.logger.Trace().
		Str("request_id", sRep.RequestID).
		Str("source", p.Header.Source).
		Msgf("search reply received")
	// update naming store and catalog
	for _, resp := range sRep.Responses {
		c.node.conf.Storage.GetNamingStore().Set(resp.Name, []byte(resp.Metahash))
		c.node.UpdateCatalog(resp.Metahash, p.Header.Source)
		for _, chunk := range resp.Chunks {
			if chunk != nil {
				c.node.UpdateCatalog(string(chunk), p.Header.Source)
			}
		}
	}

	value, ok := c.node.searchReplyCh.Load(sRep.RequestID)
	if !ok {
		c.node.logger.Trace().
			Str("request_id", sRep.RequestID).
			Str("packet_id", p.Header.PacketID).
			Msgf("not waiting for reply")
	}
	searchReplyCh := value.(chan []types.FileInfo)
	select {
	case searchReplyCh <- sRep.Responses:
	default:
		c.node.logger.Warn().Str("request_id", sRep.RequestID).Msgf("blocking reply")
	}
	return nil
}

// logging is a utility function that adds logging in a handler
func logging(logger *zerolog.Logger) func(registry.Exec) registry.Exec {
	return func(next registry.Exec) registry.Exec {
		return func(m types.Message, p transport.Packet) error {
			newlogger := logger.With().
				Str("source", p.Header.Source).
				Str("packet_id", p.Header.PacketID).
				Str("message_type", p.Msg.Type).
				Logger()
			if ignoreMsgTypeInLog()[p.Msg.Type] {
				newlogger = newlogger.Level(zerolog.Disabled)
			}

			defer func() {
				if v := recover(); v != nil {
					logger.Error().Msgf("panic: %v\nstack: %v", v, string(debug.Stack()))
					panic(v)
				}
			}()

			if m == nil {
				if p.Msg.Type != "empty" {
					newlogger.Warn().Msgf("nil message")
				}
				return next(m, p)
			}
			newlogger.Info().Msgf("process message: %v", m.String())
			if err := next(m, p); err != nil {
				newlogger.Error().Msgf("error when processing message: %v", err)
				return err
			}
			return nil
		}
	}
}
