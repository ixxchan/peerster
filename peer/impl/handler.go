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

// TODO: parallel processing. How to handle errors?
// TODO: Do I need to Ack to myself?
// TODO: what about using a single mutex to protect status and log?
func (c *controller) rumorsHandler(m types.Message, p transport.Packet) error {
	c.node.rumorsMu.Lock()
	defer c.node.rumorsMu.Unlock()

	rumors := m.(*types.RumorsMessage)
	newRumor := false

	// Process Rumors
	for _, rumor := range rumors.Rumors {
		if rumor.Sequence != c.node.status[rumor.Origin]+1 {
			// rumor is not expected, ignore it
			continue
		}
		newRumor = true

		// TODO: if we have multiple rumors from the same origin, we may update multiple times
		if !c.node.neighbors.has(rumor.Origin) {
			c.node.SetRoutingEntry(rumor.Origin, p.Header.RelayedBy)
		}

		newPkt := transport.Packet{
			Header: p.Header,
			Msg:    rumor.Msg,
		}

		if err := c.node.conf.MessageRegistry.ProcessPacket(newPkt); err != nil {
			return fmt.Errorf("failed to process rumor: %v", err)
		}
		// It is possible that status has already been updated concurrently
		c.node.status[rumor.Origin] = rumor.Sequence
		c.node.rumorsLog[rumor.Origin] = append(c.node.rumorsLog[rumor.Origin], rumor)
	}

	if p.Header.Source != c.node.getAddr() {
		// Send back an AckMessage to the source
		hdr := transport.NewHeader(c.node.getAddr(), c.node.getAddr(), p.Header.Source, 1)
		ackMsg := types.AckMessage{
			AckedPacketID: p.Header.PacketID,
			// TODO: before or after processing rumors?
			Status: c.node.status,
		}
		ackPkt := transport.Packet{
			Header: &hdr,
			Msg:    c.node.getMarshalledMsg(&ackMsg),
		}
		if err := c.node.conf.Socket.Send(p.Header.Source, ackPkt, timeout); err != nil {
			return fmt.Errorf("failed to send rumor ack: %v", err)
		}

		// Send the RumorMessage to another random neighbor in the case where one of the Rumor data in the packet is new
		if newRumor {
			if c.node.neighbors.len() == 0 || c.node.neighbors.len() == 1 && c.node.neighbors.has(p.Header.Source) {
				return nil
			}
			neighbor := c.node.neighbors.getRandom()
			for neighbor == p.Header.Source {
				neighbor = c.node.neighbors.getRandom()
			}

			hdr := transport.NewHeader(c.node.getAddr(), c.node.getAddr(), neighbor, 1)
			p.Header = &hdr
			if err := c.node.conf.Socket.Send(neighbor, p, timeout); err != nil {
				return fmt.Errorf("failed to send rumor: %v", err)
			}
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

	c.node.logger.Debug().Msgf("status %v, my status %v", remoteStatus, c.node.status)

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
		if err := c.node.conf.Socket.Send(p.Header.Source, pkt, timeout); err != nil {
			c.node.logger.Trace().Msgf("failed to send status: %v", err)
		}
	}

	if remoteMissing != nil {
		hdr := transport.NewHeader(c.node.getAddr(), c.node.getAddr(), p.Header.Source, 0)
		pkt := transport.Packet{
			Header: &hdr,
			Msg:    c.node.getMarshalledMsg(&types.RumorsMessage{Rumors: remoteMissing}),
		}
		if err := c.node.conf.Socket.Send(p.Header.Source, pkt, timeout); err != nil {
			c.node.logger.Trace().Msgf("failed to send rumors: %v", err)
		}
		// Do not wait for the possible ack message
	}

	if !remoteHasNew && remoteMissing == nil {
		if rand.Float64() < c.node.conf.ContinueMongering {

			// send a status message to a random neighbor
			neighbor := c.node.neighbors.getRandom()
			if neighbor == p.Header.Source && c.node.neighbors.len() == 1 {
				return nil
			}
			for neighbor == p.Header.Source {
				neighbor = c.node.neighbors.getRandom()
			}

			hdr := transport.NewHeader(c.node.getAddr(), c.node.getAddr(), neighbor, 0)
			pkt := transport.Packet{
				Header: &hdr,
				Msg:    c.node.getMarshalledMsg((*types.StatusMessage)(&c.node.status)),
			}

			if err := c.node.conf.Socket.Send(neighbor, pkt, timeout); err != nil {
				c.node.logger.Trace().Msgf("failed to send status: %v", err)
			}
		}
	}

	return nil
}

func (c *controller) ackHandler(m types.Message, p transport.Packet) error {
	ack := m.(*types.AckMessage)
	// Each packet should only be acked once
	select {
	case c.node.ackCh[ack.AckedPacketID] <- struct{}{}:
		close(c.node.ackCh[ack.AckedPacketID])
		delete(c.node.ackCh, ack.AckedPacketID)
		c.node.logger.Debug().Msgf("ack %v received", ack.AckedPacketID)
	default:
		c.node.logger.Warn().Str("packet_id", ack.AckedPacketID).Msgf("blocking ack")
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
			return fmt.Errorf("failed to process rumor: %v", err)
		}
	}
	return nil
}
