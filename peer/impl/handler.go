package impl

import (
	"fmt"
	"math/rand"

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

		if !routingUpdated[rumor.Origin] && !c.node.neighbors.has(rumor.Origin) {
			c.node.SetRoutingEntry(rumor.Origin, p.Header.RelayedBy)
			routingUpdated[rumor.Origin] = true
		}

		newPkt := transport.Packet{
			Header: p.Header,
			Msg:    rumor.Msg,
		}

		if p.Header.Source == c.node.getAddr() {
			if err := c.node.conf.MessageRegistry.ProcessPacket(newPkt); err != nil {
				return err
			}
		} else {
			go c.node.conf.MessageRegistry.ProcessPacket(newPkt)
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

	c.node.logger.Trace().Msgf("status %v, my status %v", remoteStatus, c.node.status)

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
			c.node.logger.Debug().Msgf("ack %v received", ack.AckedPacketID)
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
