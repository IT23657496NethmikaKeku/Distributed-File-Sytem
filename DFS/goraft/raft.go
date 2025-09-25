package goraft

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path"
	"sync"
	"time"
)

func Assert[T comparable](msg string, a, b T) {
	if a != b {
		panic(fmt.Sprintf("%s. Got a = %#v, b = %#v", msg, a, b))
	}
}

type StateMachine interface {
	Apply(cmd []byte) ([]byte, error)
}

type ApplyResult struct {
	Result []byte
	Error  error
}

type Entry struct {
	Command []byte
	Term    uint64
	result  chan ApplyResult
}

type RPCMessage struct {
	Term uint64
}

type RequestVoteRequest struct {
	RPCMessage
	CandidateId  uint64
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteResponse struct {
	RPCMessage
	VoteGranted bool
}

type AppendEntriesRequest struct {
	RPCMessage
	LeaderId     uint64
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Entry
	LeaderCommit uint64
}

type AppendEntriesResponse struct {
	RPCMessage
	Success bool
}

type ClusterMember struct {
	Id         uint64
	Address    string
	nextIndex  uint64
	matchIndex uint64
	votedFor   uint64
	rpcClient  *rpc.Client
}

type ServerState string

const (
	leaderState    ServerState = "leader"
	followerState              = "follower"
	candidateState             = "candidate"
)

type Server struct {
	done   bool
	server *http.Server
	Debug  bool

	mu          sync.Mutex
	currentTerm uint64
	log         []Entry

	id               uint64
	address          string
	electionTimeout  time.Time
	heartbeatMs      int
	heartbeatTimeout time.Time
	statemachine     StateMachine
	metadataDir      string
	fd               *os.File

	commitIndex  uint64
	lastApplied  uint64
	state        ServerState
	cluster      []ClusterMember
	clusterIndex int
}

func min[T ~int | ~uint64](a, b T) T {
	if a < b {
		return a
	}
	return b
}

func max[T ~int | ~uint64](a, b T) T {
	if a > b {
		return a
	}
	return b
}

func (s *Server) debugmsg(msg string) string {
	return fmt.Sprintf("%s [Id: %d, Term: %d, State: %s] %s",
		time.Now().Format("15:04:05.000"), s.id, s.currentTerm, s.state, msg)
}

func (s *Server) debug(msg string) {
	if !s.Debug {
		return
	}
	fmt.Println(s.debugmsg(msg))
}

func (s *Server) debugf(msg string, args ...any) {
	if !s.Debug {
		return
	}
	s.debug(fmt.Sprintf(msg, args...))
}

func (s *Server) warn(msg string) {
	fmt.Println("[WARN] " + s.debugmsg(msg))
}

func Server_assert[T comparable](s *Server, msg string, a, b T) {
	Assert(s.debugmsg(msg), a, b)
}

func NewServer(
	clusterConfig []ClusterMember,
	statemachine StateMachine,
	metadataDir string,
	clusterIndex int,
) *Server {
	var cluster []ClusterMember
	for _, c := range clusterConfig {
		if c.Id == 0 {
			panic("Id must not be 0.")
		}
		cluster = append(cluster, c)
	}

	return &Server{
		id:           cluster[clusterIndex].Id,
		address:      cluster[clusterIndex].Address,
		cluster:      cluster,
		statemachine: statemachine,
		metadataDir:  metadataDir,
		clusterIndex: clusterIndex,
		heartbeatMs:  150, // Reduced from 300ms for faster elections
		mu:           sync.Mutex{},
		Debug:        false, // Will be enabled in main.go
	}
}

const PAGE_SIZE = 4096
const ENTRY_HEADER = 16
const ENTRY_SIZE = 128

func (s *Server) persist(writeLog bool, nNewEntries int) {
	if nNewEntries == 0 && writeLog {
		nNewEntries = len(s.log)
	}

	s.fd.Seek(0, 0)

	var page [PAGE_SIZE]byte
	binary.LittleEndian.PutUint64(page[:8], s.currentTerm)
	binary.LittleEndian.PutUint64(page[8:16], s.getVotedFor())
	binary.LittleEndian.PutUint64(page[16:24], uint64(len(s.log)))

	n, err := s.fd.Write(page[:])
	if err != nil {
		panic(err)
	}
	Server_assert(s, "Wrote full page", n, PAGE_SIZE)

	if writeLog && nNewEntries > 0 {
		newLogOffset := max(len(s.log)-nNewEntries, 0)
		s.fd.Seek(int64(PAGE_SIZE+ENTRY_SIZE*newLogOffset), 0)
		bw := bufio.NewWriter(s.fd)

		var entryBytes [ENTRY_SIZE]byte
		for i := newLogOffset; i < len(s.log); i++ {
			if len(s.log[i].Command) > ENTRY_SIZE-ENTRY_HEADER {
				panic(fmt.Sprintf("Command too large (%d). Max: %d bytes.",
					len(s.log[i].Command), ENTRY_SIZE-ENTRY_HEADER))
			}

			binary.LittleEndian.PutUint64(entryBytes[:8], s.log[i].Term)
			binary.LittleEndian.PutUint64(entryBytes[8:16], uint64(len(s.log[i].Command)))
			copy(entryBytes[16:], s.log[i].Command)

			n, err := bw.Write(entryBytes[:])
			if err != nil {
				panic(err)
			}
			Server_assert(s, "Wrote full entry", n, ENTRY_SIZE)
		}

		err = bw.Flush()
		if err != nil {
			panic(err)
		}
	}

	if err := s.fd.Sync(); err != nil {
		panic(err)
	}
	s.debugf("Persisted: Term=%d, LogLen=%d (%d new), VotedFor=%d",
		s.currentTerm, len(s.log), nNewEntries, s.getVotedFor())
}

func (s *Server) ensureLog() {
	if len(s.log) == 0 {
		s.log = append(s.log, Entry{})
	}
}

func (s *Server) setVotedFor(id uint64) {
	s.cluster[s.clusterIndex].votedFor = id
}

func (s *Server) getVotedFor() uint64 {
	return s.cluster[s.clusterIndex].votedFor
}

func (s *Server) Metadata() string {
	return fmt.Sprintf("md_%d.dat", s.id)
}

func (s *Server) restore() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.fd == nil {
		var err error
		s.fd, err = os.OpenFile(
			path.Join(s.metadataDir, s.Metadata()),
			os.O_SYNC|os.O_CREATE|os.O_RDWR,
			0755)
		if err != nil {
			panic(err)
		}
	}

	s.fd.Seek(0, 0)

	var page [PAGE_SIZE]byte
	n, err := s.fd.Read(page[:])
	if err == io.EOF {
		s.ensureLog()
		return
	} else if err != nil {
		panic(err)
	}
	Server_assert(s, "Read full page", n, PAGE_SIZE)

	s.currentTerm = binary.LittleEndian.Uint64(page[:8])
	s.setVotedFor(binary.LittleEndian.Uint64(page[8:16]))
	lenLog := binary.LittleEndian.Uint64(page[16:24])
	s.log = nil

	if lenLog > 0 {
		s.fd.Seek(int64(PAGE_SIZE), 0)

		for i := 0; uint64(i) < lenLog; i++ {
			var entryBytes [ENTRY_SIZE]byte
			n, err := s.fd.Read(entryBytes[:])
			if err != nil {
				panic(err)
			}
			Server_assert(s, "Read full entry", n, ENTRY_SIZE)

			e := Entry{
				Term: binary.LittleEndian.Uint64(entryBytes[:8]),
			}
			lenValue := binary.LittleEndian.Uint64(entryBytes[8:16])
			e.Command = make([]byte, lenValue)
			copy(e.Command, entryBytes[16:16+lenValue])
			s.log = append(s.log, e)
		}
	}

	s.ensureLog()
	s.debugf("Restored: Term=%d, LogLen=%d, VotedFor=%d",
		s.currentTerm, len(s.log), s.getVotedFor())
}

func (s *Server) requestVote() {
	for i := range s.cluster {
		if i == s.clusterIndex {
			continue
		}

		go func(i int) {
			s.mu.Lock()
			req := RequestVoteRequest{
				RPCMessage:   RPCMessage{Term: s.currentTerm},
				CandidateId:  s.id,
				LastLogIndex: uint64(len(s.log) - 1),
				LastLogTerm:  s.log[len(s.log)-1].Term,
			}
			s.debugf("Requesting vote from node %d", s.cluster[i].Id)
			s.mu.Unlock()

			var rsp RequestVoteResponse
			ok := s.rpcCall(i, "Server.HandleRequestVoteRequest", req, &rsp)
			if !ok {
				return
			}

			s.mu.Lock()
			defer s.mu.Unlock()

			if s.updateTerm(rsp.RPCMessage) {
				return
			}

			if rsp.Term == req.Term && rsp.VoteGranted {
				s.debugf("Vote granted by node %d", s.cluster[i].Id)
				s.cluster[i].votedFor = s.id
			}
		}(i)
	}
}

func (s *Server) HandleRequestVoteRequest(req RequestVoteRequest, rsp *RequestVoteResponse) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.updateTerm(req.RPCMessage)
	s.debugf("Vote request from node %d (term %d)", req.CandidateId, req.Term)

	rsp.VoteGranted = false
	rsp.Term = s.currentTerm

	if req.Term < s.currentTerm {
		s.debugf("Rejecting vote: stale term")
		return nil
	}

	lastLogTerm := s.log[len(s.log)-1].Term
	logLen := uint64(len(s.log) - 1)
	logOk := req.LastLogTerm > lastLogTerm ||
		(req.LastLogTerm == lastLogTerm && req.LastLogIndex >= logLen)

	grant := req.Term == s.currentTerm &&
		logOk &&
		(s.getVotedFor() == 0 || s.getVotedFor() == req.CandidateId)

	if grant {
		s.debugf("Granting vote to node %d", req.CandidateId)
		s.setVotedFor(req.CandidateId)
		rsp.VoteGranted = true
		s.resetElectionTimeout()
		s.persist(false, 0)
	} else {
		s.debugf("Rejecting vote from node %d", req.CandidateId)
	}

	return nil
}

func (s *Server) updateTerm(msg RPCMessage) bool {
	if msg.Term > s.currentTerm {
		s.debugf("Updating term: %d -> %d", s.currentTerm, msg.Term)
		s.currentTerm = msg.Term
		s.state = followerState
		s.setVotedFor(0)
		s.resetElectionTimeout()
		s.persist(false, 0)
		return true
	}
	return false
}

func (s *Server) HandleAppendEntriesRequest(req AppendEntriesRequest, rsp *AppendEntriesResponse) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.updateTerm(req.RPCMessage)

	if req.Term == s.currentTerm && s.state == candidateState {
		s.debug("Converting to follower (received AppendEntries from leader)")
		s.state = followerState
	}

	rsp.Term = s.currentTerm
	rsp.Success = false

	if req.Term < s.currentTerm {
		s.debugf("Rejecting AppendEntries from node %d: stale term", req.LeaderId)
		return nil
	}

	if s.state != followerState {
		s.debugf("Rejecting AppendEntries: not a follower")
		return nil
	}

	s.resetElectionTimeout()

	logLen := uint64(len(s.log))
	validPreviousLog := req.PrevLogIndex == 0 ||
		(req.PrevLogIndex < logLen && s.log[req.PrevLogIndex].Term == req.PrevLogTerm)

	if !validPreviousLog {
		s.debugf("Rejecting AppendEntries: invalid previous log")
		return nil
	}

	// Process entries
	next := req.PrevLogIndex + 1
	nNewEntries := 0

	for i := next; i < next+uint64(len(req.Entries)); i++ {
		e := req.Entries[i-next]

		if i >= uint64(cap(s.log)) {
			newTotal := next + uint64(len(req.Entries))
			newLog := make([]Entry, i, newTotal*2)
			copy(newLog, s.log)
			s.log = newLog
		}

		if i < uint64(len(s.log)) && s.log[i].Term != e.Term {
			s.log = s.log[:i]
		}

		if i < uint64(len(s.log)) {
			Server_assert(s, "Existing log matches", s.log[i].Term, e.Term)
		} else {
			s.log = append(s.log, e)
			nNewEntries++
		}
	}

	if req.LeaderCommit > s.commitIndex {
		s.commitIndex = min(req.LeaderCommit, uint64(len(s.log)-1))
	}

	s.persist(nNewEntries != 0, nNewEntries)
	rsp.Success = true

	if len(req.Entries) > 0 {
		s.debugf("Accepted %d entries from leader %d", len(req.Entries), req.LeaderId)
	}

	return nil
}

var ErrApplyToLeader = errors.New("Cannot apply message to follower, apply to leader")

func (s *Server) Apply(commands [][]byte) ([]ApplyResult, error) {
	s.mu.Lock()

	if s.state != leaderState {
		s.mu.Unlock()
		return nil, ErrApplyToLeader
	}

	s.debugf("Processing %d new commands", len(commands))
	resultChans := make([]chan ApplyResult, len(commands))

	for i, command := range commands {
		resultChans[i] = make(chan ApplyResult)
		s.log = append(s.log, Entry{
			Term:    s.currentTerm,
			Command: command,
			result:  resultChans[i],
		})
	}

	s.persist(true, len(commands))
	s.mu.Unlock()

	s.appendEntries()

	results := make([]ApplyResult, len(commands))
	var wg sync.WaitGroup
	wg.Add(len(commands))

	for i, ch := range resultChans {
		go func(i int, c chan ApplyResult) {
			results[i] = <-c
			wg.Done()
		}(i, ch)
	}

	wg.Wait()
	return results, nil
}

func (s *Server) rpcCall(i int, name string, req, rsp any) bool {
	s.mu.Lock()
	c := s.cluster[i]
	var err error

	if c.rpcClient == nil {
		c.rpcClient, err = rpc.DialHTTP("tcp", c.Address)
		if err == nil {
			s.cluster[i].rpcClient = c.rpcClient // Store the connection
		}
	}

	rpcClient := c.rpcClient
	s.mu.Unlock()

	if err == nil && rpcClient != nil {
		err = rpcClient.Call(name, req, rsp)
	}

	if err != nil {
		// Only log errors occasionally to reduce spam
		if rand.Intn(10) == 0 {
			s.warn(fmt.Sprintf("RPC error to node %d: %s", c.Id, err))
		}

		// Close bad connection
		s.mu.Lock()
		if s.cluster[i].rpcClient != nil {
			s.cluster[i].rpcClient.Close()
			s.cluster[i].rpcClient = nil
		}
		s.mu.Unlock()
	}

	return err == nil
}

const MAX_APPEND_ENTRIES_BATCH = 8000

func (s *Server) appendEntries() {
	for i := range s.cluster {
		if i == s.clusterIndex {
			continue
		}

		go func(i int) {
			s.mu.Lock()

			next := s.cluster[i].nextIndex
			prevLogIndex := next - 1
			prevLogTerm := s.log[prevLogIndex].Term

			var entries []Entry
			if uint64(len(s.log)-1) >= s.cluster[i].nextIndex {
				entries = s.log[next:]
			}

			if len(entries) > MAX_APPEND_ENTRIES_BATCH {
				entries = entries[:MAX_APPEND_ENTRIES_BATCH]
			}

			req := AppendEntriesRequest{
				RPCMessage:   RPCMessage{Term: s.currentTerm},
				LeaderId:     s.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: s.commitIndex,
			}

			s.mu.Unlock()

			var rsp AppendEntriesResponse
			ok := s.rpcCall(i, "Server.HandleAppendEntriesRequest", req, &rsp)
			if !ok {
				return
			}

			s.mu.Lock()
			defer s.mu.Unlock()

			if s.updateTerm(rsp.RPCMessage) {
				return
			}

			if rsp.Success {
				s.cluster[i].nextIndex = max(prevLogIndex+uint64(len(entries))+1, 1)
				s.cluster[i].matchIndex = s.cluster[i].nextIndex - 1
				if len(entries) > 0 {
					s.debugf("Node %d accepted %d entries", s.cluster[i].Id, len(entries))
				}
			} else {
				s.cluster[i].nextIndex = max(s.cluster[i].nextIndex-1, 1)
				s.debugf("Node %d rejected, backing off to index %d", s.cluster[i].Id, s.cluster[i].nextIndex)
			}
		}(i)
	}
}

func (s *Server) advanceCommitIndex() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.state == leaderState {
		lastLogIndex := uint64(len(s.log) - 1)

		for i := lastLogIndex; i > s.commitIndex; i-- {
			quorum := len(s.cluster)/2 + 1
			for j := range s.cluster {
				if quorum == 0 {
					break
				}

				if j == s.clusterIndex || s.cluster[j].matchIndex >= i {
					quorum--
				}
			}

			if quorum == 0 && s.log[i].Term == s.currentTerm {
				s.commitIndex = i
				s.debugf("New commit index: %d", i)
				break
			}
		}
	}

	for s.lastApplied < s.commitIndex {
		s.lastApplied++
		entry := s.log[s.lastApplied]

		if len(entry.Command) > 0 {
			s.debugf("Applying entry %d", s.lastApplied)
			res, err := s.statemachine.Apply(entry.Command)

			if entry.result != nil {
				entry.result <- ApplyResult{Result: res, Error: err}
			}
		}
	}
}

func (s *Server) resetElectionTimeout() {
	interval := time.Duration(rand.Intn(s.heartbeatMs*2) + s.heartbeatMs*2)
	s.electionTimeout = time.Now().Add(interval * time.Millisecond)
	s.debugf("Election timeout reset: %dms", interval)
}

func (s *Server) timeout() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if time.Now().After(s.electionTimeout) {
		s.debug("Election timeout - starting new election")
		s.state = candidateState
		s.currentTerm++
		s.setVotedFor(s.id)

		for i := range s.cluster {
			if i != s.clusterIndex {
				s.cluster[i].votedFor = 0
			}
		}

		s.resetElectionTimeout()
		s.persist(false, 0)
		s.requestVote()
	}
}

func (s *Server) becomeLeader() {
	s.mu.Lock()
	defer s.mu.Unlock()

	quorum := len(s.cluster)/2 + 1
	votes := 0

	for i := range s.cluster {
		if s.cluster[i].votedFor == s.id {
			votes++
		}
	}

	if votes >= quorum {
		s.debug("BECAME LEADER")
		s.state = leaderState

		for i := range s.cluster {
			s.cluster[i].nextIndex = uint64(len(s.log))
			s.cluster[i].matchIndex = 0
		}

		// Commit no-op entry
		s.log = append(s.log, Entry{Term: s.currentTerm, Command: nil})
		s.persist(true, 1)
		s.heartbeatTimeout = time.Now()
	}
}

func (s *Server) heartbeat() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if time.Now().After(s.heartbeatTimeout) {
		s.heartbeatTimeout = time.Now().Add(time.Duration(s.heartbeatMs) * time.Millisecond)
		s.debug("Sending heartbeat")
		s.appendEntries()
	}
}

func (s *Server) Start() {
	s.mu.Lock()
	s.state = followerState
	s.done = false
	s.mu.Unlock()

	s.restore()

	// Start RPC server
	rpcServer := rpc.NewServer()
	rpcServer.Register(s)
	l, err := net.Listen("tcp", s.address)
	if err != nil {
		panic(err)
	}

	mux := http.NewServeMux()
	mux.Handle(rpc.DefaultRPCPath, rpcServer)
	s.server = &http.Server{Handler: mux}
	go s.server.Serve(l)

	s.debug("Raft server started")

	// Main state machine loop
	go func() {
		s.mu.Lock()
		s.resetElectionTimeout()
		s.mu.Unlock()

		for {
			s.mu.Lock()
			if s.done {
				s.mu.Unlock()
				return
			}
			state := s.state
			s.mu.Unlock()

			switch state {
			case leaderState:
				s.heartbeat()
				s.advanceCommitIndex()
			case followerState:
				s.timeout()
				s.advanceCommitIndex()
			case candidateState:
				s.timeout()
				s.becomeLeader()
			}

			// CRITICAL FIX: Add sleep to prevent tight loop
			time.Sleep(200 * time.Millisecond)
		}
	}()
}
