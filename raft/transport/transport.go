package transport

import (
	"github.com/coreos/etcd/raft/raftpb"

	"context"
	"go.uber.org/zap"
	"sync"
)

// Raft node has an id and is capable of handling raft messages
type RaftNode interface {
	Id() uint64
	Process(ctx context.Context, msg raftpb.Message) error
}

// Transport is an abstraction over actual communication protocol. All
// that is needed to know is that it can accept peers and route
// messages to them
type Transport interface {
	Send(msgs []raftpb.Message)
	AddPeer(peer RaftNode)
	RemovePeer(peer RaftNode)
}

var log, _ = zap.NewDevelopment()
var local = localTransport{map[uint64]RaftNode{}, new(sync.Mutex), log}

type localTransport struct {
	peers map[uint64]RaftNode
	mu    *sync.Mutex
	log   *zap.Logger
}

func NewTransport() *localTransport {
	return &local
}

// Implements Transport
func (t *localTransport) AddPeer(peer RaftNode) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.peers[peer.Id()] = peer
}

// Implements Transport
func (t *localTransport) RemovePeer(peer RaftNode) {
	t.mu.Lock()
	defer t.mu.Unlock()

	delete(t.peers, peer.Id())
}

// Implements Transport
func (t *localTransport) Send(msgs []raftpb.Message) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for i := range msgs {
		msg := msgs[i]

		if to, ok := t.peers[msg.To]; ok {
			go to.Process(context.TODO(), msg)
		}
	}
}
