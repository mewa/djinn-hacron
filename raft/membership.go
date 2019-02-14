package raft

import (
	"github.com/coreos/etcd/raft"
	"sync"
	"bytes"
)

type membership struct {
	members map[uint64]*raft.Peer
	mu *sync.RWMutex
}

func newMembership() *membership {
	return &membership{
		members: make(map[uint64]*raft.Peer),
		mu: new(sync.RWMutex),
	}
}

func (p *membership) addPeer(peer *raft.Peer) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.members[peer.ID] = peer
}

func (p *membership) peers() []raft.Peer {
	p.mu.RLock()
	p.mu.RUnlock()

	var peers []raft.Peer
	for _, v := range p.members {
		peers = append(peers, *v)
	}
	return peers
}

func (p *membership) getId(id uint64) *raft.Peer {
	p.mu.RLock()
	defer p.mu.RUnlock()

	peer, ok := p.members[id]
	if ok {
		return peer
	}
	return nil
}

func (p *membership) getUrl(context []byte) *raft.Peer {
	p.mu.RLock()
	defer p.mu.RUnlock()

	for _, v := range p.members {
		if bytes.Equal(v.Context, context) {
			return v
		}
	}
	return nil
}


func (p *membership) removeId(id uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	delete(p.members, id)
}

func (p *membership) removeUrl(id uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	delete(p.members, id)
}
