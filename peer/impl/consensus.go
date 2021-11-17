package impl

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"sync"
	"time"

	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/storage"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
)

type MultiPaxos struct {
	tlc *TLC
	p   *Proposer
	a   *Acceptor
}

func NewMultiPaxos(n *node, conf peer.Configuration) MultiPaxos {
	tlc := NewTLC(n, conf)
	p := newProposer(n, tlc, conf)
	a := &Acceptor{
		n:             n,
		tlc:           tlc,
		maxID:         0,
		acceptedID:    0,
		acceptedValue: nil,
	}
	tlc.p = p

	return MultiPaxos{
		tlc: tlc,
		p:   p,
		a:   a,
	}
}

func (mp *MultiPaxos) PrepareAndPropose(value types.PaxosValue) (*types.PaxosValue, error) {

	mp.tlc.mu.Lock()
	advanceCh := make(chan struct{}, 1)
	mp.tlc.advanceCh = advanceCh
	mp.tlc.mu.Unlock()
	if err := mp.p.prepareAndPropose(value); err != nil {
		return nil, err
	}
	// block until TLC advances, so that we move to the next instance
	<-advanceCh
	return mp.a.acceptedValue, nil
}

func (mp *MultiPaxos) prepareHandler(m types.Message, p transport.Packet) error {
	return mp.a.prepareHandler(m, p)
}

func (mp *MultiPaxos) proposeHandler(m types.Message, p transport.Packet) error {
	return mp.a.proposeHandler(m, p)
}

func (mp *MultiPaxos) acceptHandler(m types.Message, p transport.Packet) error {
	accept := m.(*types.PaxosAcceptMessage)

	mp.p.mu.Lock()
	defer mp.p.mu.Unlock()

	step := mp.p.tlc.GetCurrStep()

	// outdated promise
	if accept.Step != step {
		mp.p.n.logger.Info().Msgf("TLC step mismatch: from %s, step %d, currStep %d", p.Header.Source, accept.Step, step)
		return nil
	}
	// Note: should use acceptor's maxID instead of proposer's currID, in case the node didn't propose.
	// This is also why we do not use p.acceptHandler
	if accept.ID != mp.a.maxID {
		mp.p.n.logger.Info().Msgf("ID %d != maxID %d", accept.ID, mp.a.maxID)
		return nil
	}

	mp.p.acceptCnt[accept.Value.UniqID]++
	if mp.p.acceptCnt[accept.Value.UniqID] >= mp.p.threshold {
		// consensus reached
		select {
		case mp.p.consensusCh <- struct{}{}:
		default:
		}
		mp.p.tlc.Broadcast(&accept.Value, step)
	}

	return nil
}

func (mp *MultiPaxos) promiseHandler(m types.Message, p transport.Packet) error {
	return mp.p.promiseHandler(m, p)
}

func (mp *MultiPaxos) tlcHandler(m types.Message, p transport.Packet) error {
	return mp.tlc.tlcHandler(m, p)
}

type TLC struct {
	n        *node
	currStep uint

	threshold int

	blockchain  storage.Store
	namingStore storage.Store
	received    map[uint]struct {
		cnt   *int
		block *types.BlockchainBlock
	}
	sent      bool
	mu        sync.Mutex
	advanceCh chan struct{}

	p *Proposer
}

func NewTLC(n *node, conf peer.Configuration) *TLC {
	return &TLC{
		n:         n,
		currStep:  0,
		threshold: conf.PaxosThreshold(conf.TotalPeers),

		blockchain:  conf.Storage.GetBlockchainStore(),
		namingStore: conf.Storage.GetNamingStore(),
		received: make(map[uint]struct {
			cnt   *int
			block *types.BlockchainBlock
		}),
	}
}

func (t *TLC) GetCurrStep() uint {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.currStep
}

// grab lock before calling
func (t *TLC) advanceCurrStep() {
	t.currStep++
	t.sent = false
	t.p.advance()
	select {
	case t.advanceCh <- struct{}{}:
	default:
	}
}

// grab lock before calling
func (t *TLC) broadcast(block *types.BlockchainBlock) {
	msg := types.TLCMessage{
		Step:  t.currStep,
		Block: *block,
	}
	tMsg := *t.n.getMarshalledMsg(msg)
	go t.n.Broadcast(tMsg)
}

// called when consuses is reached
func (t *TLC) Broadcast(value *types.PaxosValue, step uint) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if step != t.currStep || t.sent {
		return
	}
	t.sent = true

	block := types.BlockchainBlock{
		Value:    *value,
		Index:    step,
		PrevHash: t.blockchain.Get(storage.LastBlockKey),
	}

	if block.PrevHash == nil {
		var empty [32]byte
		block.PrevHash = empty[:]
	}

	// compute hash
	// H = Index || v.UniqID || v.Filename || v.Metahash || Prevhash
	var buf []byte
	buf = append(buf, []byte(strconv.Itoa(int(block.Index)))...)
	buf = append(buf, []byte(block.Value.UniqID)...)
	buf = append(buf, []byte(block.Value.Filename)...)
	buf = append(buf, []byte(block.Value.Metahash)...)
	buf = append(buf, []byte(block.PrevHash)...)
	hash := sha256.Sum256(buf)
	block.Hash = hash[:]

	t.broadcast(&block)
}

func (t *TLC) tlcHandler(m types.Message, pkt transport.Packet) error {
	msg := m.(*types.TLCMessage)

	// grab lock before get step, since the msg may advance step
	t.n.logger.Trace().Msgf("tlcHandler grabbing lock")
	t.mu.Lock()
	t.n.logger.Trace().Msgf("tlcHandler got lock")
	defer t.mu.Unlock()

	currStep := t.currStep
	if msg.Step < currStep {
		t.n.logger.Trace().Msgf("ignore old message: %v, curr:", msg.Step, currStep)
		return nil
	}
	s, ok := t.received[msg.Step]
	if !ok {
		cnt := 1
		s.cnt = &cnt
		s.block = &msg.Block
		t.received[msg.Step] = s
	} else {
		*t.received[msg.Step].cnt++
		if !bytes.Equal(msg.Block.Hash, s.block.Hash) {
			t.n.logger.Error().Msg("different block")
			return fmt.Errorf("different block")
		}
	}
	t.n.logger.Trace().Msgf("received TLC for step: %v, collected: %v/%v", msg.Step, *s.cnt, t.threshold)

	do := func(broadcast bool, block *types.BlockchainBlock) error {
		buf, err := block.Marshal()
		if err != nil {
			t.n.logger.Error().Msgf("failed to marshal block: %v", err)
			return fmt.Errorf("failed to marshal block: %v", err)
		}
		t.blockchain.Set(hex.EncodeToString(block.Hash), buf)
		t.blockchain.Set(storage.LastBlockKey, block.Hash)
		t.namingStore.Set(block.Value.Filename, []byte(block.Value.Metahash))

		if broadcast && !t.sent {
			t.broadcast(block)
			t.sent = true
		}

		t.n.logger.Trace().Msgf("advancing TLC step: %v->%v", currStep, currStep+1)
		t.advanceCurrStep()
		delete(t.received, currStep)
		currStep++
		return nil
	}

	s, ok = t.received[currStep]
	if !ok {
		return nil
	}
	t.n.logger.Trace().Msgf("checking currStep: %v, collected: %v/%v", currStep, *s.cnt, t.threshold)
	if *s.cnt >= t.threshold {
		if err := do(true, s.block); err != nil {
			return err
		}
	} else {
		return nil
	}

	// Catchup up if necessary
	for {
		s, ok = t.received[currStep]
		if !ok {
			t.n.logger.Trace().Msgf("checking currStep: %v, collected: 0/%v", currStep, t.threshold)
			return nil
		}
		t.n.logger.Trace().Msgf("checking currStep: %v, collected: %v/%v", currStep, *s.cnt, t.threshold)
		if *s.cnt >= t.threshold {
			if err := do(false, s.block); err != nil {
				return err
			}
		} else {
			return nil
		}
	}
}

type Proposer struct {
	n          *node
	retry      time.Duration
	threshold  int
	totalPeers uint

	tlc    *TLC
	currID uint

	mu          sync.Mutex
	promiseCh   chan struct{}
	acceptCnt   map[string]int
	consensusCh chan struct{}
}

func newProposer(n *node, tlc *TLC, conf peer.Configuration) *Proposer {
	return &Proposer{
		n:          n,
		retry:      conf.PaxosProposerRetry,
		threshold:  conf.PaxosThreshold(conf.TotalPeers),
		totalPeers: conf.TotalPeers,

		tlc:    tlc,
		currID: conf.PaxosID,

		mu:          sync.Mutex{},
		acceptCnt:   make(map[string]int),
		consensusCh: make(chan struct{}, 1),
	}

}

// Returns when observed a consensus reached (not necessarily the same as the proposed value)
func (p *Proposer) prepareAndPropose(value types.PaxosValue) error {
	p.mu.Lock()
	consensusCh := p.consensusCh
	p.mu.Unlock()
	for {
		if err := p.prepare(); err != nil {
			return err
		}

		if err := p.propose(value); err != nil {
			return err
		}

		select {
		case <-p.n.ctx.Done():
			return nil
		case <-consensusCh:
			return nil
		case <-time.After(p.retry):
		}

		p.mu.Lock()
		p.currID += p.totalPeers
		p.mu.Unlock()
	}
}

// prepare until threshold promises are collected
func (p *Proposer) prepare() error {
	p.mu.Lock()
	currStep := p.tlc.GetCurrStep()
	prepare := types.PaxosPrepareMessage{
		Step:   currStep,
		ID:     p.currID,
		Source: p.n.getAddr(),
	}
	promiseCh := make(chan struct{}, p.threshold)
	p.promiseCh = promiseCh
	p.mu.Unlock()

	msg := *p.n.getMarshalledMsg(prepare)
	if err := p.n.Broadcast(msg); err != nil {
		p.n.logger.Error().Err(err).Msg("failed to broadcast prepare")
		return err
	}

	for !p.collectPromises(promiseCh) {
		p.n.logger.Trace().Msg("retry prepare")
		p.mu.Lock()
		if p.tlc.GetCurrStep() != currStep {
			p.mu.Unlock()
			return nil
		}
		p.currID += p.totalPeers
		prepare.ID = p.currID
		promiseCh = make(chan struct{}, p.threshold)
		p.promiseCh = promiseCh
		p.mu.Unlock()
		msg := *p.n.getMarshalledMsg(prepare)
		if err := p.n.Broadcast(msg); err != nil {
			p.n.logger.Error().Err(err).Msg("failed to broadcast prepare")
			return err
		}
	}
	return nil
}

func (p *Proposer) collectPromises(ch <-chan struct{}) bool {
	done := make(chan struct{})
	cancel := make(chan struct{})
	go func() {
		// collect threshold promises
		for i := 0; i < p.threshold; i++ {
			select {
			case <-cancel:
				return
			case <-ch:
			}
		}
		close(done)
	}()

	select {
	case <-done:
		p.n.logger.Trace().Msg("collected threshold promises")
		return true
	case <-time.After(p.retry):
		close(cancel)
		p.n.logger.Trace().Msg("promises not enough")
		return false
	}
}

// propose once
func (p *Proposer) propose(value types.PaxosValue) error {
	p.mu.Lock()
	propose := types.PaxosProposeMessage{
		Step:  p.tlc.GetCurrStep(),
		ID:    p.currID,
		Value: value,
	}
	p.mu.Unlock()

	msg := *p.n.getMarshalledMsg(propose)
	if err := p.n.Broadcast(msg); err != nil {
		p.n.logger.Error().Err(err).Msg("failed to broadcast propose")
		return err
	}
	return nil
}

func (p *Proposer) promiseHandler(m types.Message, pkt transport.Packet) error {
	promise := m.(*types.PaxosPromiseMessage)

	p.mu.Lock()
	defer p.mu.Unlock()

	// outdated promise
	if promise.Step != p.tlc.GetCurrStep() {
		p.n.logger.Info().Msgf("TLC step mismatch: from %s, step %d, currStep %d", pkt.Header.Source, promise.Step, p.tlc.GetCurrStep())
		return nil
	}
	if promise.ID != p.currID {
		p.n.logger.Info().Msgf("ID %d != currID %d", promise.ID, p.currID)
		return nil
	}

	select {
	case p.promiseCh <- struct{}{}:
	default:
	}

	return nil
}

// reset internal state, move to new instance
func (p *Proposer) advance() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.consensusCh = make(chan struct{}, 1)
	p.acceptCnt = make(map[string]int)
}

type Acceptor struct {
	n     *node
	tlc   *TLC
	maxID uint

	acceptedID    uint
	acceptedValue *types.PaxosValue

	mu sync.Mutex
}

func (a *Acceptor) prepareHandler(m types.Message, p transport.Packet) error {
	prepare := m.(*types.PaxosPrepareMessage)

	var promise types.PaxosPromiseMessage
	{
		a.mu.Lock()
		if prepare.Step != a.tlc.GetCurrStep() {
			a.n.logger.Info().Msgf("TLC step mismatch: from %s, step %d, currStep %d", p.Header.Source, prepare.Step, a.tlc.GetCurrStep())
			a.mu.Unlock()
			return nil
		}
		if prepare.ID <= a.maxID {
			a.n.logger.Info().Msgf("ID %d <= maxID %d", prepare.ID, a.maxID)
			a.mu.Unlock()
			return nil
		}
		a.maxID = prepare.ID
		promise = types.PaxosPromiseMessage{
			Step:          prepare.Step,
			ID:            prepare.ID,
			AcceptedID:    a.acceptedID,
			AcceptedValue: a.acceptedValue,
		}
		a.mu.Unlock()
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
	go func() {
		if err := a.n.Broadcast(*tMsg); err != nil {
			a.n.logger.Error().Err(err).Msg("failed to broadcast promise")
		}
	}()
	return nil
}

func (a *Acceptor) proposeHandler(m types.Message, p transport.Packet) error {
	propose := m.(*types.PaxosProposeMessage)
	a.mu.Lock()
	defer a.mu.Unlock()
	if propose.Step != a.tlc.GetCurrStep() {
		a.n.logger.Info().Msgf("TLC step mismatch: from %s, step %d, currStep %d", p.Header.Source, propose.Step, a.tlc.GetCurrStep())
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
		Step:  a.tlc.GetCurrStep(),
		ID:    a.acceptedID,
		Value: propose.Value,
	}
	a.n.logger.Trace().Uint("tlc_step", accept.Step).
		Uint("paxos_id", accept.ID).
		Msgf("accepted %v %v", a.acceptedID, a.acceptedValue.String())
	tMsgAccept := *a.n.getMarshalledMsg(accept)
	go func() {
		if err := a.n.Broadcast(tMsgAccept); err != nil {
			a.n.logger.Error().Err(err).Msg("failed to broadcast accept")
		}
	}()
	return nil
}
