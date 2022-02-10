package impl

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"

	"github.com/sasha-s/go-deadlock"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/storage"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
)

// TODO: make the relationship between TLC and MultiPaxos more clear
type MultiPaxos struct {
	node *node
	conf peer.Configuration

	mu  deadlock.Mutex
	tlc *TLC
	p   *Proposer
	a   *Acceptor

	doneCh chan<- struct{}
}

func NewMultiPaxos(n *node, conf peer.Configuration) *MultiPaxos {
	tlc := &TLC{
		n:         n,
		currStep:  0,
		threshold: conf.PaxosThreshold(conf.TotalPeers),

		blockchain:  conf.Storage.GetBlockchainStore(),
		namingStore: conf.Storage.GetNamingStore(),
		received: make(map[uint]struct {
			cnt   *int
			block *types.BlockchainBlock
		}),

		advanceCh: []chan types.PaxosValue{make(chan types.PaxosValue, 1)},
	}
	doneCh := make(chan struct{})
	p, a := newPaxosInstance(n, tlc.currStep, conf, doneCh)
	tlc.p = p

	mp := &MultiPaxos{
		node: n,
		conf: conf,

		tlc:    tlc,
		p:      p,
		a:      a,
		doneCh: doneCh,
	}

	tlc.mp = mp
	return mp
}

func (mp *MultiPaxos) getInstance() (*Proposer, *Acceptor) {
	mp.mu.Lock()
	defer mp.mu.Unlock()
	return mp.p, mp.a
}

func (mp *MultiPaxos) advanceInstance(step uint) {
	mp.mu.Lock()
	defer mp.mu.Unlock()
	close(mp.doneCh)

	doneCh := make(chan struct{})
	mp.doneCh = doneCh
	mp.p, mp.a = newPaxosInstance(mp.node, step, mp.conf, doneCh)
}

func (mp *MultiPaxos) PrepareAndPropose(value types.PaxosValue) (*types.PaxosValue, error) {
	p, _ := mp.getInstance()
	mp.tlc.mu.Lock()
	advanceCh := mp.tlc.advanceCh[p.step]
	mp.tlc.mu.Unlock()
	if err := p.prepareAndPropose(value); err != nil {
		return nil, err
	}
	// block until TLC advances, so that we move to the next instance
	v := <-advanceCh
	return &v, nil
}

func (mp *MultiPaxos) prepareHandler(m types.Message, p transport.Packet) error {
	_, a := mp.getInstance()
	return a.prepareHandler(m, p)
}

func (mp *MultiPaxos) proposeHandler(m types.Message, p transport.Packet) error {
	_, a := mp.getInstance()
	return a.proposeHandler(m, p)
}

func (mp *MultiPaxos) acceptHandler(m types.Message, pkt transport.Packet) error {
	p, _ := mp.getInstance()
	decided, err := p.acceptHandler(m, pkt)
	if err != nil {
		return err
	}
	if decided != nil {
		mp.tlc.Broadcast(decided)
	}
	return nil
}

func (mp *MultiPaxos) promiseHandler(m types.Message, pkt transport.Packet) error {
	p, _ := mp.getInstance()
	return p.promiseHandler(m, pkt)
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
	mu        deadlock.Mutex
	advanceCh []chan types.PaxosValue

	p  *Proposer
	mp *MultiPaxos
}

// grab lock before calling
func (t *TLC) advanceCurrStep() {
	t.currStep++
	t.sent = false
	t.mp.advanceInstance(t.currStep)
	t.advanceCh = append(t.advanceCh, make(chan types.PaxosValue, 1))
	t.advanceCh[t.currStep-1] <- t.received[t.currStep-1].block.Value
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
func (t *TLC) Broadcast(value *types.PaxosValue) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.sent = true

	block := types.BlockchainBlock{
		Value:    *value,
		Index:    t.currStep,
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
	t.mu.Lock()
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

func newPaxosInstance(n *node, step uint, conf peer.Configuration, doneCh <-chan struct{}) (*Proposer, *Acceptor) {
	p := &Proposer{
		node:       n,
		retry:      conf.PaxosProposerRetry,
		threshold:  conf.PaxosThreshold(conf.TotalPeers),
		totalPeers: conf.TotalPeers,

		step:   step,
		currID: conf.PaxosID,

		mu:          deadlock.Mutex{},
		acceptCnt:   make(map[string]int),
		consensusCh: make(chan struct{}, 1),
		doneCh:      doneCh,
	}
	a := &Acceptor{
		node:          n,
		step:          step,
		maxID:         0,
		acceptedID:    0,
		acceptedValue: nil,
		doneCh:        doneCh,
	}
	return p, a
}

type promiseChMsg struct {
	acceptedID    uint
	acceptedValue *types.PaxosValue
}
type Proposer struct {
	*node

	retry      time.Duration
	threshold  int
	totalPeers uint

	step   uint
	currID uint

	mu          deadlock.Mutex
	promiseCh   chan promiseChMsg
	acceptCnt   map[string]int
	consensusCh chan struct{}

	proposing bool
	doneCh    <-chan struct{} // This instance is passed
}

// Returns when observed a consensus reached (not necessarily the same as the proposed value)
func (p *Proposer) prepareAndPropose(value types.PaxosValue) error {
	p.mu.Lock()
	p.proposing = true
	consensusCh := p.consensusCh
	p.mu.Unlock()
	for {
		acceptedValue, err := p.prepare()
		if err != nil {
			return err
		}

		if acceptedValue != nil {
			err = p.propose(*acceptedValue)
		} else {
			err = p.propose(value)
		}

		if err != nil {
			return err
		}

		select {
		case <-p.ctx.Done():
			return nil
		case <-consensusCh:
			return nil
		case <-p.doneCh:
			return nil
		case <-time.After(p.retry):
		}

		p.mu.Lock()
		p.currID += p.totalPeers
		p.mu.Unlock()
	}
}

// prepare until threshold promises are collected OR accept observed
func (p *Proposer) prepare() (*types.PaxosValue, error) {
	p.mu.Lock()
	prepare := types.PaxosPrepareMessage{
		Step:   p.step,
		ID:     p.currID,
		Source: p.getAddr(),
	}
	promiseCh := make(chan promiseChMsg, p.threshold)
	p.promiseCh = promiseCh
	p.mu.Unlock()
	p.logger.Trace().Msgf("begin to prepare %v %v", prepare.Step, prepare.ID)

	msg := *p.getMarshalledMsg(prepare)
	if err := p.Broadcast(msg); err != nil {
		p.logger.Error().Err(err).Msg("failed to broadcast prepare")
		return nil, nil
	}

	for {
		b, acceptedValue := p.collectPromises(promiseCh)
		if b {
			return acceptedValue, nil
		}
		p.logger.Trace().Msgf("retry prepare, step %v", prepare.Step)

		// early exit
		select {
		case <-p.ctx.Done():
			return nil, nil
		case <-p.doneCh:
			return nil, nil
		default:
		}

		p.mu.Lock()
		p.currID += p.totalPeers
		prepare.ID = p.currID
		promiseCh = make(chan promiseChMsg, p.threshold)
		p.promiseCh = promiseCh
		p.mu.Unlock()
		msg := *p.getMarshalledMsg(prepare)
		if err := p.Broadcast(msg); err != nil {
			p.logger.Error().Err(err).Msg("failed to broadcast prepare")
			return nil, err
		}
	}
}

func (p *Proposer) collectPromises(ch <-chan promiseChMsg) (bool, *types.PaxosValue) {
	done := make(chan struct{})
	cancel := make(chan struct{})

	var acceptedID uint
	var acceptedValue *types.PaxosValue

	go func() {
		// collect threshold promises
		for i := 0; i < p.threshold; i++ {
			select {
			case <-cancel:
				return
			case msg := <-ch:
				if msg.acceptedID > acceptedID {
					acceptedID = msg.acceptedID
					acceptedValue = msg.acceptedValue
				}
			}
		}
		close(done)
	}()

	select {
	case <-done:
		p.logger.Trace().Msg("collected threshold promises")
		return true, acceptedValue
	case <-time.After(p.retry):
		close(cancel)
		p.logger.Trace().Msg("promises not enough")
		return false, nil
	}
}

// propose once
func (p *Proposer) propose(value types.PaxosValue) error {
	p.mu.Lock()
	propose := types.PaxosProposeMessage{
		Step:  p.step,
		ID:    p.currID,
		Value: value,
	}
	p.mu.Unlock()

	msg := *p.getMarshalledMsg(propose)
	if err := p.Broadcast(msg); err != nil {
		p.logger.Error().Err(err).Msg("failed to broadcast propose")
		return err
	}
	return nil
}

func (p *Proposer) promiseHandler(m types.Message, pkt transport.Packet) error {
	promise := m.(*types.PaxosPromiseMessage)

	p.mu.Lock()
	defer p.mu.Unlock()

	// outdated promise
	if promise.Step != p.step {
		p.logger.Info().Msgf("TLC step mismatch: from %s, step %d, currStep %d", pkt.Header.Source, promise.Step, p.step)
		return nil
	}
	if promise.ID != p.currID {
		p.logger.Info().Msgf("ID %d != currID %d", promise.ID, p.currID)
		return nil
	}

	select {
	case p.promiseCh <- struct {
		acceptedID    uint
		acceptedValue *types.PaxosValue
	}{promise.AcceptedID, promise.AcceptedValue}:
	default:
		// not collecting promises
	}

	return nil
}

// Note: If the node didn't propose, it should also collect accepts.
func (p *Proposer) acceptHandler(m types.Message, pkt transport.Packet) (decided *types.PaxosValue, err error) {
	accept := m.(*types.PaxosAcceptMessage)

	p.mu.Lock()
	defer p.mu.Unlock()

	step := p.step

	// outdated promise
	if accept.Step != step {
		p.logger.Info().Msgf("TLC step mismatch: from %s, step %d, currStep %d", pkt.Header.Source, accept.Step, step)
		return nil, nil
	}

	p.acceptCnt[accept.Value.UniqID]++
	p.logger.Trace().Msgf("Accepted %d times", p.acceptCnt[accept.Value.UniqID])
	if p.acceptCnt[accept.Value.UniqID] >= p.threshold {
		// consensus reached
		p.logger.Info().Msgf("Consensus reached: %s", accept.Value.UniqID)
		select {
		case p.consensusCh <- struct{}{}:
		default:
		}
		return &accept.Value, nil
	}
	return nil, nil
}

type Acceptor struct {
	*node

	step  uint
	maxID uint

	acceptedID    uint
	acceptedValue *types.PaxosValue

	mu     deadlock.Mutex
	doneCh <-chan struct{} // This instance is passed
}

func (a *Acceptor) prepareHandler(m types.Message, p transport.Packet) error {
	prepare := m.(*types.PaxosPrepareMessage)

	var promise types.PaxosPromiseMessage
	{
		a.mu.Lock()
		if prepare.Step != a.step {
			a.logger.Info().Msgf("TLC step mismatch: from %s, step %d, currStep %d", p.Header.Source, prepare.Step, a.step)
			a.mu.Unlock()
			return nil
		}
		if prepare.ID <= a.maxID {
			a.logger.Info().Msgf("ID %d <= maxID %d", prepare.ID, a.maxID)
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

	a.logger.Trace().
		Uint("tlc_step", promise.Step).
		Uint("paxos_id", promise.ID).
		Uint("accepted_id", promise.AcceptedID).
		Msgf("promised")
	tMsgPromise := a.getMarshalledMsg(promise)
	privateMsg := types.PrivateMessage{
		Recipients: map[string]struct{}{prepare.Source: {}},
		Msg:        tMsgPromise,
	}
	tMsg := a.getMarshalledMsg(privateMsg)
	go func() {
		if err := a.Broadcast(*tMsg); err != nil {
			a.logger.Error().Err(err).Msg("failed to broadcast promise")
		}
	}()
	return nil
}

func (a *Acceptor) proposeHandler(m types.Message, p transport.Packet) error {
	propose := m.(*types.PaxosProposeMessage)
	a.mu.Lock()
	defer a.mu.Unlock()
	if propose.Step != a.step {
		a.logger.Info().Msgf("TLC step mismatch: from %s, step %d, currStep %d", p.Header.Source, propose.Step, a.step)
		return nil
	}
	if propose.ID != a.maxID {
		// If the ID > maxID, i.e., we have not observed a prepare message for this ID, we also reject the propose.
		a.logger.Info().Msgf("ID %d != maxID %d", propose.ID, a.maxID)
		return nil
	}
	a.acceptedID = propose.ID
	a.acceptedValue = &propose.Value
	accept := types.PaxosAcceptMessage{
		Step:  a.step,
		ID:    a.acceptedID,
		Value: propose.Value,
	}
	a.logger.Trace().Uint("tlc_step", accept.Step).
		Uint("paxos_id", accept.ID).
		Msgf("accepted %v %v", a.acceptedID, a.acceptedValue.String())
	tMsgAccept := *a.getMarshalledMsg(accept)
	go func() {
		if err := a.Broadcast(tMsgAccept); err != nil {
			a.logger.Error().Err(err).Msg("failed to broadcast accept")
		}
	}()
	return nil
}
