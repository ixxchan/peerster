package impl

import (
	"time"

	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
)

type TLC struct {
	currStep uint
}

type Proposer struct {
	n      *node
	tlc    *TLC
	currID uint
	retry  time.Duration
}

func (p *Proposer) Prepare() {
	prepare := types.PaxosPrepareMessage{
		Step:   p.tlc.currStep,
		ID:     p.currID,
		Source: p.n.getAddr(),
	}
	msg := *p.n.getMarshalledMsg(prepare)
	p.n.Broadcast(msg)
}

func (p *Proposer) Propose(types.PaxosProposeMessage) {
}

func (p *Proposer) handlePromiseMsg(m types.Message, pkt transport.Packet) error {
	// promise := types.PaxosPromiseMessage{}
	return nil
}

type Acceptor struct {
	n     *node
	tlc   *TLC
	maxID uint

	acceptedID    uint
	acceptedValue *types.PaxosValue
}

func (a *Acceptor) prepareHandler(m types.Message, p transport.Packet) error {
	prepare := m.(*types.PaxosPrepareMessage)
	if prepare.Step != a.tlc.currStep {
		a.n.logger.Info().Msgf("TLC step mismatch: from %s, step %d, currStep %d", p.Header.Source, prepare.Step, a.tlc.currStep)
		return nil
	}
	if prepare.ID <= a.maxID {
		a.n.logger.Info().Msgf("ID %d <= maxID %d", prepare.ID, a.maxID)
		return nil
	}
	println(3)
	a.maxID = prepare.ID
	promise := types.PaxosPromiseMessage{
		Step:          prepare.Step,
		ID:            prepare.ID,
		AcceptedID:    a.acceptedID,
		AcceptedValue: a.acceptedValue,
	}
	a.n.logger.Trace().
		Uint("tlc_step", promise.Step).
		Uint("paxos_id", promise.ID).
		Uint("accepted_id", promise.AcceptedID).
		Msgf("promised")
	tMsgPromise := a.n.getMarshalledMsg(promise)
	privateMsg := types.PrivateMessage{
		Recipients: map[string]struct{}{prepare.Source: {}},
		Msg:        tMsgPromise,
	}
	tMsg := a.n.getMarshalledMsg(privateMsg)
	a.n.logger.Trace().Msgf("broadcasting...")
	a.n.Broadcast(*tMsg)
	a.n.logger.Trace().Msgf("broadcasted")
	return nil
}

func (a *Acceptor) proposeHandler(m types.Message, p transport.Packet) error {
	propose := m.(*types.PaxosProposeMessage)
	if propose.Step != a.tlc.currStep {
		a.n.logger.Info().Msgf("TLC step mismatch: from %s, step %d, currStep %d", p.Header.Source, propose.Step, a.tlc.currStep)
		return nil
	}
	if propose.ID != a.maxID {
		// If the ID > maxID, i.e., we have not observed a prepare message for this ID, we also reject the propose.
		a.n.logger.Info().Msgf("ID %d != maxID %d", propose.ID, a.maxID)
		return nil
	}
	a.acceptedID = propose.ID
	a.acceptedValue = &propose.Value
	accept := types.PaxosAcceptMessage{
		Step:  a.tlc.currStep,
		ID:    a.acceptedID,
		Value: propose.Value,
	}
	a.n.logger.Trace().Uint("tlc_step", accept.Step).
		Uint("paxos_id", accept.ID).
		Msgf("accepted %v %v", a.acceptedID, a.acceptedValue.String())
	tMsgAccept := *a.n.getMarshalledMsg(accept)
	return a.n.Broadcast(tMsgAccept)
}
