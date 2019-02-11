package raft

import (
	"context"
	"github.com/coreos/etcd/pkg/idutil"
	"github.com/coreos/etcd/pkg/wait"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	t "github.com/mewa/djinn/raft/transport"
	"go.uber.org/zap"
	"time"
	"errors"
	"bytes"
	m "github.com/mewa/djinn/raft/messages"
	"github.com/golang/protobuf/proto"
)

type raftNode struct {
	id    uint64

	peers []raft.Peer
	leader uint64

	storage   *raft.MemoryStorage
	transport t.Transport
	node      raft.Node

	idGen *idutil.Generator
	w     wait.Wait

	done chan struct{}

	log *zap.Logger
	ticker *time.Ticker
}

func NewRaftNode(id int, peers []string, heartbeat time.Duration) *raftNode {
	raftPeers := make([]raft.Peer, len(peers))

	for i := 0; i < len(peers); i++ {
		raftPeers[i] = raft.Peer{
			ID: uint64(i) + 1,
			Context: []byte(peers[i]),
		}
	}

	logger, _ := zap.NewDevelopment()

	rn := &raftNode{
		id:    uint64(id),
		peers: raftPeers,

		idGen: idutil.NewGenerator(uint16(id), time.Now()),
		w:     wait.New(),

		log:  logger,
		ticker: time.NewTicker(heartbeat),
		done: make(chan struct{}),
	}

	rn.start()

	return rn
}

func (rn *raftNode) start() {
	storage := raft.NewMemoryStorage()

	c := &raft.Config{
		ID:              rn.id,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
	}

	rn.transport = t.NewTransport()
	rn.transport.AddPeer(rn)

	rn.storage = storage
	rn.node = raft.StartNode(c, rn.peers)

	go rn.raftLoop()
}

func (rn *raftNode) Stop() {
	rn.done <- struct{}{}
}

func (rn *raftNode) configure(ctx context.Context, cc raftpb.ConfChange) error {
	cc.ID = rn.idGen.Next()
	ch := rn.w.Register(cc.ID)

	if err := rn.node.ProposeConfChange(ctx, cc); err != nil {
		rn.w.Trigger(cc.ID, nil)
		return err
	}

	select {
	case val := <-ch:
		var err error

		switch val.(type) {
		case error:
			err = val.(error)
		}

		return err
	case <-ctx.Done():
		rn.w.Trigger(cc.ID, nil)
		return ctx.Err()
	case <-rn.done:
		return nil
	}
}

func (rn *raftNode) AddMember(id uint64, url string) error {
	err := rn.configure(context.TODO(), raftpb.ConfChange{
		Type:   raftpb.ConfChangeAddNode,
		NodeID: id,
		Context: []byte(url),
	})

	if err != nil {
		rn.log.Info("attempted to add member",
			zap.Uint64("id", rn.id),
			zap.Uint64("added_id", id),
			zap.Error(err),
		)
	} else {
		rn.log.Info("added member",
			zap.Uint64("id", rn.id),
			zap.Uint64("added_id", id),
		)
	}

	return err
}

func (rn *raftNode) RemoveMember(id uint64) error {
	// TODO: add checks for leadership
	err := rn.configure(context.TODO(), raftpb.ConfChange{
		Type:   raftpb.ConfChangeRemoveNode,
		NodeID: id,
	})

	rn.log.Info("removed member",
		zap.Uint64("id", rn.id),
		zap.Uint64("removed_id", id),
		zap.Error(err),
	)
	return err
}

func (rn *raftNode) Propose(data []byte) error {
	ctx, _ := context.WithTimeout(context.TODO(), 5 * time.Second)

	id := rn.idGen.Next()

	payload, err := proto.Marshal(&m.Message{
		Id: id,
		Data: data,
	})
	if err != nil {
		rn.log.Error("Could not serialize message", zap.Error(err))
	}

	ch := rn.w.Register(id)

	if err := rn.node.Propose(ctx, payload); err != nil {
		rn.w.Trigger(id, err)
	}

	select {
	case v := <-ch:
		switch v.(type) {
		case error:
			return v.(error)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (rn *raftNode) processSoftState(softState *raft.SoftState) {
	if softState == nil {
		return
	}

	rn.leader = softState.Lead
}

func (rn *raftNode) raftLoop() {
	for {
		select {
		case <-rn.ticker.C:
			rn.node.Tick()
		case rd := <-rn.node.Ready():
			rn.processSoftState(rd.SoftState)

			rn.saveToStorage(rd.HardState, rd.Entries, rd.Snapshot)
			rn.transport.Send(rd.Messages)

			if !raft.IsEmptySnap(rd.Snapshot) {
				rn.processSnapshot(rd.Snapshot)
			}

			for _, entry := range rd.CommittedEntries {
				if entry.Type == raftpb.EntryNormal {
					rn.process(entry)
				} else if entry.Type == raftpb.EntryConfChange {
					var cc raftpb.ConfChange
					cc.Unmarshal(entry.Data)

					if err := rn.applyConfChange(cc); err == nil {
						rn.w.Trigger(cc.ID, cc)
					} else {
						rn.w.Trigger(cc.ID, err)
					}

				}
			}
			rn.node.Advance()
		case <-rn.done:
			rn.Stop()
			return
		}
	}
}

func (rn *raftNode) saveToStorage(hardState raftpb.HardState, entries []raftpb.Entry, snapshot raftpb.Snapshot) {
	rn.storage.Append(entries)

	if !raft.IsEmptyHardState(hardState) {
		rn.storage.SetHardState(hardState)
	}

	if !raft.IsEmptySnap(snapshot) {
		rn.storage.ApplySnapshot(snapshot)
	}
}

func (rn *raftNode) applyConfChange(cc raftpb.ConfChange) error {
	switch cc.Type {
	case raftpb.ConfChangeAddNode:
		rn.log.Info("Add node",
			zap.Uint64("id", rn.id),
			zap.Uint64("added_id", cc.NodeID),
		)

		return rn.addPeer(cc)
	case raftpb.ConfChangeRemoveNode:
		rn.log.Info("Remove node",
			zap.Uint64("id", rn.id),
			zap.Uint64("removed_id", cc.NodeID),
		)

		return rn.removePeer(cc)
	}
	return nil
}

func (rn *raftNode) addPeer(cc raftpb.ConfChange) error {
	for _, p := range rn.peers {
		if bytes.Equal(p.Context, cc.Context) {
			cc.NodeID = 0
			break
		}
	}

	rn.node.ApplyConfChange(cc)

	if cc.NodeID == 0 {
		return errors.New("Peer already exists")
	}

	rn.peers = append(rn.peers, raft.Peer{ID: cc.NodeID, Context: cc.Context})

	return nil
}

func (rn *raftNode) removePeer(cc raftpb.ConfChange) error {
	rn.node.ApplyConfChange(cc)

	if cc.NodeID == rn.id {
		rn.log.Info("Removed self", zap.Uint64("id", rn.id), zap.Uint64("leader", rn.leader))

		rn.done <- struct{}{}
		rn.transport.RemovePeer(rn)
	}

	return nil
}

func (rn *raftNode) process(entry raftpb.Entry) {
	// TODO: add logic
	rn.log.Info("Raft entry",
		zap.Uint64("id", rn.id),
		zap.Stringer("entry", &entry),
	)

	var msg m.Message
	err := proto.Unmarshal(entry.Data, &msg)

	rn.w.Trigger(msg.Id, msg)
}

func (rn *raftNode) processSnapshot(snap raftpb.Snapshot) {
	rn.storage.ApplySnapshot(snap)
}

// Implements t.RaftNode
func (rn *raftNode) Id() uint64 {
	return rn.id
}

// Implements t.RaftNode
func (rn *raftNode) Process(ctx context.Context, msg raftpb.Message) error {
	return rn.node.Step(ctx, msg)
}
