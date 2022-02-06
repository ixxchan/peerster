package impl

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/xid"
	"github.com/rs/zerolog"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
)

func ignoreMsgTypeInLog() map[string]bool {
	empty := types.EmptyMessage{}
	status := types.StatusMessage{}
	return map[string]bool{
		empty.Name():  true,
		status.Name(): true,
	}
}

// envLogLevel is the name of the environment variable to change the logging
// level.
//
//   NODELOG=trace go test ./...
//   NODELOG=info go test ./...
//
const envLogLevel = "NODELOG"
const envLogFile = "NODELOG_FILE"

const defaultLevel = zerolog.Disabled

var level = defaultLevel

var logout io.Writer = zerolog.ConsoleWriter{
	Out:        os.Stdout,
	TimeFormat: time.RFC3339,
	NoColor:    true,
}

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
	logfile := os.Getenv(envLogFile)
	if logfile != "" {
		f, err := os.Create(logfile)
		if err != nil {
			fmt.Fprintln(os.Stderr, "failed to open logfile", err)
		} else {
			logout = f
			os.Stdout = f
		}
	}
}

const SocketTimeout = time.Second * 1

// NewPeer creates a new peer. You can change the content and location of this
// function but you MUST NOT change its signature and package location.
func NewPeer(conf peer.Configuration) peer.Peer {
	ctx, cancelFunc := context.WithCancel(context.Background())

	node := &node{
		conf:         conf,
		routingTable: NewConcurrentMapString(),
		status:       make(map[string]uint),
		rumorsLog:    make(map[string][]types.Rumor),
		catalog:      NewConcurrentMapSet(),

		ctx:        ctx,
		cancelFunc: cancelFunc,
	}
	// initialize logger first, so that other components can use it
	node.logger = zerolog.New(logout).Level(level).With().
		Timestamp().
		Caller().
		Str("role", "node").
		Str("myaddr", node.getAddr()).Logger()

	node.multiPaxos = NewMultiPaxos(node, conf)

	ctrl := NewController(node)
	conf.MessageRegistry.RegisterMessageCallback(types.ChatMessage{}, logging(&node.logger)(ctrl.chatHandler))
	conf.MessageRegistry.RegisterMessageCallback(types.EmptyMessage{}, logging(&node.logger)(ctrl.emptyHandler))
	conf.MessageRegistry.RegisterMessageCallback(types.PrivateMessage{}, logging(&node.logger)(ctrl.privateHandler))

	conf.MessageRegistry.RegisterMessageCallback(types.RumorsMessage{}, logging(&node.logger)(ctrl.rumorsHandler))
	conf.MessageRegistry.RegisterMessageCallback(types.StatusMessage{}, logging(&node.logger)(ctrl.statusHandler))
	conf.MessageRegistry.RegisterMessageCallback(types.AckMessage{}, logging(&node.logger)(ctrl.ackHandler))

	conf.MessageRegistry.RegisterMessageCallback(types.DataRequestMessage{}, logging(&node.logger)(ctrl.dataRequestHandler))
	conf.MessageRegistry.RegisterMessageCallback(types.DataReplyMessage{}, logging(&node.logger)(ctrl.dataReplyHandler))
	conf.MessageRegistry.RegisterMessageCallback(types.SearchReplyMessage{}, logging(&node.logger)(ctrl.searchReplyHandler))
	conf.MessageRegistry.RegisterMessageCallback(types.SearchRequestMessage{}, logging(&node.logger)(ctrl.searchRequestHandler))

	conf.MessageRegistry.RegisterMessageCallback(types.PaxosPrepareMessage{}, logging(&node.logger)(node.multiPaxos.prepareHandler))
	conf.MessageRegistry.RegisterMessageCallback(types.PaxosProposeMessage{}, logging(&node.logger)(node.multiPaxos.proposeHandler))
	// TODO: proposer
	conf.MessageRegistry.RegisterMessageCallback(types.PaxosAcceptMessage{}, logging(&node.logger)(node.multiPaxos.acceptHandler))
	conf.MessageRegistry.RegisterMessageCallback(types.PaxosPromiseMessage{}, logging(&node.logger)(node.multiPaxos.promiseHandler))
	conf.MessageRegistry.RegisterMessageCallback(types.TLCMessage{}, logging(&node.logger)(node.multiPaxos.tlcHandler))

	// routingTable[myAddr] = myAddr
	node.AddPeer(conf.Socket.GetAddress())
	return node
}

// node implements a peer to build a Peerster system
//
// - implements peer.Peer
type node struct {
	conf peer.Configuration

	// ****** utility fields ********

	ctx        context.Context
	cancelFunc context.CancelFunc
	logger     zerolog.Logger

	// ******** common fields ********

	routingTable ConcurrentMapString
	neighbors    neighbors

	// ******** rumor fields ********

	// rumor sequence number, equal to the number of rumors this node has created
	//
	// should use atomic operation to get/set
	seq uint32

	// a node’s view on the system. By “view” we mean all the rumors the
	// node has processed so far.
	//
	// map peer -> the sequence number of the last Rumor processed from that peer.
	status    map[string]uint
	rumorsLog map[string][]types.Rumor
	// protect status & rumorsLog
	rumorsMu sync.Mutex

	// PacketID -> chan struct{}
	//
	// This is used in Broadcast() and ackHandler().
	// They do not require rumorsMu, and accessing the whole map is not needed, so simply use sync.Map.
	ackCh sync.Map

	// ******** data sharing fields ********

	catalog ConcurrentMapSet
	// RequestID -> struct {ch chan []byte, key string}
	dataReplyCh sync.Map
	// RequestID -> []FileInfo
	searchReplyCh    sync.Map
	handledSearchReq sync.Map

	// ******** paxos fields ********
	multiPaxos *MultiPaxos
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
			select {
			case <-n.ctx.Done():
				return
			default:
			}

			pkt, err := n.conf.Socket.Recv(time.Second * 1)
			if err != nil {
				if !errors.Is(err, transport.TimeoutErr(0)) {
					n.logger.Warn().Msgf("error when receive packet: %v", err)
				}
				continue
			}
			pktLogger := n.logger.With().
				Str("source", pkt.Header.Source).
				Str("destination", pkt.Header.Destination).
				Str("packet_id", pkt.Header.PacketID).
				Str("message_type", pkt.Msg.Type).
				Logger()
			if ignoreMsgTypeInLog()[pkt.Msg.Type] {
				pktLogger = pktLogger.Level(zerolog.Disabled)
			}

			pktLogger.Trace().Msgf("received packet")
			if pkt.Header.Destination != n.getAddr() {
				nextHop := n.routingTable.Get(pkt.Header.Destination)
				if nextHop == "" {
					pktLogger.Info().Msgf("unknown relay destination")
				} else {
					pktLogger.Trace().Msgf("relay packet")
					pkt.Header.RelayedBy = n.getAddr()
					go func() {
						if err := n.conf.Socket.Send(nextHop, pkt, SocketTimeout); err != nil {
							pktLogger.Trace().Msgf("error when send packet: %v", err)
						}
					}()
				}
			} else {
				go n.conf.MessageRegistry.ProcessPacket(pkt)
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

	return n.conf.Socket.Send(nextHop, pkt, SocketTimeout)
}

// Broadcast implements peer.Messaging
func (n *node) Broadcast(msg transport.Message) error {
	seq := atomic.AddUint32(&n.seq, 1)
	rumors := types.RumorsMessage{Rumors: []types.Rumor{
		{
			Origin:   n.getAddr(),
			Sequence: uint(seq),
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
		// Restore to the state before the operation
		atomic.StoreUint32(&n.seq, seq-1)
		return fmt.Errorf("failed to process rumor locally: %v", err)
	}

	// sends the rumor to a random neighbor.
	if n.neighbors.len() == 0 {
		n.logger.Warn().Msgf("has none neighbors to broadcast")
		return nil
	}
	neighbor := n.neighbors.getRandom()
	hdr = transport.NewHeader(n.getAddr(), n.getAddr(), neighbor, 1)
	pkt.Header = &hdr

	ackCh := make(chan struct{}, 1)
	n.ackCh.Store(pkt.Header.PacketID, ackCh)

	if err := n.conf.Socket.Send(neighbor, pkt, SocketTimeout); err != nil {
		return fmt.Errorf("failed to send rumor: %v", err)
	}
	n.logger.Debug().Str("packet_id", pkt.Header.PacketID).Msgf("rumor is sent to %v", neighbor)

	// wait for ack. If no ack, send to another neighbor
	go func() {
		if n.conf.AckTimeout == 0 {
			n.logger.Debug().Msgf("broadcast wait for ack %v, neighbor %v", pkt.Header.PacketID, pkt.Header.Destination)
			select {
			case <-ackCh:
				return
			case <-n.ctx.Done():
				return
			}

		}
		for {
			n.logger.Debug().Msgf("broadcast wait for ack %v, neighbor %v", pkt.Header.PacketID, pkt.Header.Destination)
			select {
			case <-ackCh:
				return
			case <-n.ctx.Done():
				return
			case <-time.After(n.conf.AckTimeout):
				n.ackCh.Delete(pkt.Header.PacketID)

				// send the RumorMessage to another random neighbor
				if n.neighbors.hasOnly(neighbor) {
					n.logger.Warn().Msgf("neighbor did not ack, but do not have any new neighbors")
					return
				}
				neighbor := n.neighbors.getNewRandom(neighbor)

				hdr = transport.NewHeader(n.getAddr(), n.getAddr(), neighbor, 1)
				pkt.Header = &hdr

				ackCh = make(chan struct{}, 1)
				n.ackCh.Store(pkt.Header.PacketID, ackCh)

				if err := n.conf.Socket.Send(neighbor, pkt, SocketTimeout); err != nil {
					n.logger.Error().Msgf("failed to send rumor: %v", err)
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
	n.logger.Info().Msgf("set routing entry: %v -> %v", origin, relayAddr)
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

// keep sending rumors with empty message to keep peers' routing tables up-to-date
func (n *node) heartbeat() {
	// We don't need to wait until the node has a peer, thanks to the anti-entropy mechanism

	msg := *n.getMarshalledMsg(types.EmptyMessage{})

	for {
		err := n.Broadcast(msg)
		if err != nil {
			n.logger.Trace().Msgf("heartbeat failed")
		}

		select {
		case <-time.After(n.conf.HeartbeatInterval):
			// continue loop
		case <-n.ctx.Done():
			return
		}
	}
}

// Convert types.Message (must be pointer) to *transport.Message.
// Returns nil if marshall failed.
func (n *node) getMarshalledMsg(msg types.Message) *transport.Message {
	logger := n.logger.With().CallerWithSkipFrameCount(3).Logger() // report the caller of this function
	m, err := n.conf.MessageRegistry.MarshalMessage(msg)
	if err != nil {
		logger.Error().Msgf("failed to marshall message: %v", err)
		return nil
	}
	return &m
}

// keep sending status messages to make nodes' views consistent
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
		if err := n.conf.Socket.Send(neighbor, pkt, SocketTimeout); err != nil {
			n.logger.Trace().Msgf("failed to send anti-entropy status: %v", err)
		}

		select {
		case <-time.After(n.conf.AntiEntropyInterval):
			// continue loop
		case <-n.ctx.Done():
			return
		}
	}
}

// Upload stores a new data blob on the peer and will make it available to
// other peers. The blob will be split into chunks.
//
// - Implemented in HW2
func (n *node) Upload(data io.Reader) (metahash string, err error) {
	var buf []byte
	var chunkHashHexes []string
	blobStore := n.conf.Storage.GetDataBlobStore()

	for {
		chunk := make([]byte, n.conf.ChunkSize)
		len, err := data.Read(chunk)
		if err != nil {
			if err == io.EOF {
				break
			}
			return "", fmt.Errorf("failed to read data: %v", err)
		}
		chunkHash := sha256.Sum256(chunk[:len])
		chunkHashHex := hex.EncodeToString(chunkHash[:])
		blobStore.Set(chunkHashHex, chunk[:len])

		chunkHashHexes = append(chunkHashHexes, chunkHashHex)
		buf = append(buf, chunkHash[:]...)
	}

	metahashBytes := sha256.Sum256(buf)
	metahash = hex.EncodeToString(metahashBytes[:])
	blobStore.Set(metahash, []byte(strings.Join(chunkHashHexes, peer.MetafileSep)))

	n.logger.Info().Str("metahash", metahash).Msgf("file uploaded")
	return metahash, nil
}

// implements peer.DataSharing
func (n *node) Download(metahash string) ([]byte, error) {
	n.logger.Info().Str("metahash", metahash).Msgf("file downloading")

	var file []byte
	dataCh, errCh := n.download(metahash)
	var metafile []byte
	select {
	case metafile = <-dataCh:
	case err := <-errCh:
		if strings.Contains(err.Error(), "file inexistent") {
			n.logger.Trace().Str("metahash", metahash).Err(err).Msgf("failed to download metafile")
			return nil, fmt.Errorf("failed to download metafile: inexistent")
		} else if strings.Contains(err.Error(), "timeout") {
			n.logger.Trace().Str("metahash", metahash).Err(err).Msgf("failed to download metafile")
			return nil, fmt.Errorf("failed to download metafile: timeout")
		} else {
			n.logger.Warn().Str("metahash", metahash).Err(err).Msgf("failed to download metafile")
			return nil, fmt.Errorf("failed to download metafile: %v", err)
		}
	}

	chunkHashHexes := strings.Split(string(metafile), peer.MetafileSep)

	// In the real world, several chunks can be downloaded in parallel, however,
	// for the purposes of this homework, the sequential approach must be used.
	for _, chunkHashHex := range chunkHashHexes {
		dataCh, errCh := n.download(chunkHashHex)
		select {
		case chunk := <-dataCh:
			file = append(file, chunk...)
		case err := <-errCh:
			if strings.Contains(err.Error(), "file inexistent") {
				n.logger.Trace().
					Str("metahash", metahash).
					Str("chunk_hash", chunkHashHex).
					Err(err).
					Msgf("failed to download chunk")
				return nil, fmt.Errorf("failed to download chunk: inexistent")
			} else if strings.Contains(err.Error(), "timeout") {
				n.logger.Trace().Str("metahash", metahash).
					Str("chunk_hash", chunkHashHex).
					Err(err).
					Msgf("failed to download chunk")
				return nil, fmt.Errorf("failed to download chunk: timeout")
			} else {
				n.logger.Warn().Str("metahash", metahash).
					Str("chunk_hash", chunkHashHex).
					Err(err).
					Msgf("failed to download chunk")
				return nil, fmt.Errorf("failed to download chunk: %v", err)
			}
		}
	}

	return file, nil
}

// Download a single block of data (metafile/chunk) from local storage or a peer.
// If dataCh gets a value, it is non-nil. If timeout, an error is returned.
func (n *node) download(hash string) (dataCh chan []byte, errCh chan error) {
	dataCh = make(chan []byte, 1)
	errCh = make(chan error, 1)

	blobStore := n.conf.Storage.GetDataBlobStore()
	data := blobStore.Get(hash)
	if len(data) != 0 {
		dataCh <- data
		return
	}

	peer := n.catalog.GetRandom(hash)
	if peer == "" {
		errCh <- fmt.Errorf("file inexistent")
		return
	}
	nextHop := n.routingTable.Get(peer)
	if nextHop == "" {
		errCh <- fmt.Errorf("no routing: %v", peer)
		return
	}

	req := types.DataRequestMessage{
		RequestID: xid.New().String(),
		Key:       hash,
	}
	n.logger.Trace().Str("hash", hash).
		Str("request_id", req.RequestID).
		Msgf("downloading remotely, backoff %v", n.conf.BackoffDataRequest)
	msg := n.getMarshalledMsg(&req)
	hdr := transport.NewHeader(n.getAddr(), n.getAddr(), peer, 0)
	pkt := transport.Packet{
		Header: &hdr,
		Msg:    msg,
	}

	dataReplyCh := make(chan []byte, 1)
	n.dataReplyCh.Store(req.RequestID, struct {
		ch  chan []byte
		key string
	}{dataReplyCh, hash})
	defer n.dataReplyCh.Delete(req.RequestID)

	if err := n.conf.Socket.Send(nextHop, pkt, SocketTimeout); err != nil {
		errCh <- fmt.Errorf("failed to send data request: %v", err)
		return
	}

	backoff := n.conf.BackoffDataRequest
	for i := 0; i < int(backoff.Retry); i++ {
		select {
		case <-time.After(backoff.Initial):
			backoff.Initial *= time.Duration(backoff.Factor)
		case data := <-dataReplyCh:
			if len(data) == 0 {
				errCh <- fmt.Errorf("peer returns an empty value")
				return
			} else {
				dataCh <- data
				blobStore.Set(hash, data)
				return
			}
		}
	}

	errCh <- fmt.Errorf("timeout")
	return
}

// implements peer.DataSharing
//
// This API is blocking, and calling it concurrently is undefined behavior.
func (n *node) Tag(name string, mh string) error {
	namingStore := n.conf.Storage.GetNamingStore()
	for {
		if namingStore.Get(name) != nil {
			n.logger.Error().Msgf("name already exists: %v", name)
			return fmt.Errorf("name already exists")
		}

		value, err := n.multiPaxos.PrepareAndPropose(types.PaxosValue{
			UniqID:   xid.New().String(),
			Filename: name,
			Metahash: mh,
		})

		if err != nil {
			n.logger.Error().Msgf("failed to prepare and propose: %v", err)
			return fmt.Errorf("failed to prepare and propose: %v", err)
		}

		if value.Filename == name {
			n.logger.Info().Msgf("tagged: %v", name)
			return nil
		}
	}
}

// implements peer.DataSharing
func (n *node) Resolve(name string) (metahash string) {
	value := n.conf.Storage.GetNamingStore().Get(name)
	return string(value)
}

// implements peer.DataSharing
func (n *node) GetCatalog() peer.Catalog {
	return n.catalog.GetMap()
}

// implements peer.DataSharing
func (n *node) UpdateCatalog(key string, peer string) {
	n.catalog.Add(key, peer)
}

// implements peer.DataSharing
func (n *node) SearchAll(reg regexp.Regexp, budget uint, timeout time.Duration) (names []string, err error) {
	// if budget == 0 {
	// 	return nil, fmt.Errorf("budget must be greater than 0")
	// }
	n.logger.Info().Msgf("searching for %v", reg.String())

	namesMap := make(map[string]struct{})

	for _, fileInfo := range n.searchLocal(&reg, true) {
		namesMap[fileInfo.Name] = struct{}{}
	}
	n.logger.Trace().Msgf("search local result: %v", namesMap)

	searchReplyCh, requestId, err := n.searchRemote(reg.String(), budget)
	defer n.searchReplyCh.Delete(requestId)
	if err != nil {
		return nil, err
	}

	canceled := make(chan struct{})
	done := make(chan struct{})

	// collect and merge replies
	go func() {
		defer close(done)
		for i := 0; i < int(budget); i++ {
			select {
			case reply := <-searchReplyCh:
				for _, fileInfo := range reply {
					namesMap[fileInfo.Name] = struct{}{}
				}
			case <-canceled:
				return
			}
		}
	}()

	select {
	case <-time.After(timeout):
		close(canceled)
		<-done
	case <-done:
	}

	for name := range namesMap {
		names = append(names, name)
	}
	return names, nil
}

// Divides the provided budget as evenly as possible among its neighbors. The node itself is not included.
func (n *node) divideBudget(budget uint, upstream string) map[string]uint {
	if budget == 0 {
		return nil
	}
	m := make(map[string]uint)
	neighbors := n.neighbors.getAll()

	if upstream != "" {
		// remove upstream from neighbors
		for i, neighbor := range neighbors {
			if neighbor == upstream {
				neighbors = append(neighbors[:i], neighbors[i+1:]...)
				break
			}
		}
	}
	if budget <= uint(len(neighbors)) {
		for i := 0; i < int(budget); i++ {
			m[neighbors[i]] = 1
		}
	} else {
		for i := 0; i < len(neighbors); i++ {
			m[neighbors[i]] = budget / uint(len(neighbors))
			if i == len(neighbors)-1 {
				m[neighbors[i]] -= budget % uint(len(neighbors))
			}
		}
	}
	return m
}

func (n *node) searchLocal(regexp *regexp.Regexp, includeNonexistent bool) []types.FileInfo {
	blobstore := n.conf.Storage.GetDataBlobStore()
	var fileInfos []types.FileInfo

	n.conf.Storage.GetNamingStore().ForEach(
		func(name string, metahash []byte) (continued bool) {
			if regexp.Match([]byte(name)) {
				// get metafile and chunks only locally
				metafile := blobstore.Get(string(metahash))
				if metafile == nil {
					if includeNonexistent {
						fileInfos = append(fileInfos, types.FileInfo{
							Name:     name,
							Metahash: string(metahash),
						})
					} else {
						return true
					}
				}

				chunkHashes := strings.Split(string(metafile), peer.MetafileSep)
				var chunks [][]byte
				for _, chunkHash := range chunkHashes {
					chunk := blobstore.Get(chunkHash)
					if chunk != nil {
						chunks = append(chunks, []byte(chunkHash))
					} else {
						chunks = append(chunks, nil)
					}
				}
				fileInfos = append(fileInfos, types.FileInfo{
					Name:     name,
					Metahash: string(metahash),
					Chunks:   chunks,
				})

			}
			return true
		},
	)
	return fileInfos
}

// should delete requestId from n.searchReplyCh when the search is done or aborted
func (n *node) searchRemote(reg string, budget uint) (searchReplyCh chan []types.FileInfo, requestId string, err error) {
	requestId = xid.New().String()
	searchReq := types.SearchRequestMessage{
		RequestID: requestId,
		Pattern:   reg,
		Origin:    n.getAddr(),
	}
	searchReplyCh = make(chan []types.FileInfo, budget)
	n.searchReplyCh.Store(searchReq.RequestID, searchReplyCh)
	n.handledSearchReq.Store(searchReq.RequestID, nil)

	division := n.divideBudget(budget, "")
	n.logger.Trace().
		Str("requestID", searchReq.RequestID).
		Msgf("searching remote, budget division %v", division)
	for neighbor, bgt := range division {
		req := searchReq
		req.Budget = bgt
		msg := n.getMarshalledMsg(&req)
		hdr := transport.NewHeader(n.getAddr(), n.getAddr(), neighbor, 0)
		pkt := transport.Packet{
			Header: &hdr,
			Msg:    msg,
		}

		if err := n.conf.Socket.Send(neighbor, pkt, SocketTimeout); err != nil {
			return nil, "", fmt.Errorf("failed to send search request: %v", err)
		}
	}

	return
}

// implements peer.DataSharing
func (n *node) SearchFirst(pattern regexp.Regexp, conf peer.ExpandingRing) (name string, err error) {
	fileInfos := n.searchLocal(&pattern, false)
	for _, fileInfo := range fileInfos {
		found := true
		for _, chunk := range fileInfo.Chunks {
			if chunk == nil {
				found = false
				break
			}
		}
		if found {
			return fileInfo.Name, nil
		}
	}

	patternStr := pattern.String()
	for i := 0; i < int(conf.Retry); i++ {
		searchReplyCh, requestId, err := n.searchRemote(patternStr, conf.Initial)
		defer n.searchReplyCh.Delete(requestId)
		if err != nil {
			return "", err
		}

		canceled := make(chan struct{})
		res := make(chan string, 1)

		// collect and check replies
		go func() {
			defer close(res)
			for i := 0; i < int(conf.Initial); i++ {
				select {
				case reply := <-searchReplyCh:
					for _, fileInfo := range reply {
						found := true
						for _, chunk := range fileInfo.Chunks {
							if chunk == nil {
								found = false
								break
							}
						}
						if found {
							res <- fileInfo.Name
							return
						}
					}
				case <-canceled:
					return
				}
			}
		}()

		select {
		case <-time.After(conf.Timeout):
			close(canceled)
			<-res
		case name, ok := <-res:
			if ok {
				return name, nil
			}
		}

		conf.Initial *= conf.Factor
	}

	n.logger.Info().Msgf("timeout, file not found")
	return "", nil
}
