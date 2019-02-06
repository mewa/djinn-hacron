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
)

type raftNode struct {
	id    uint64
	peers []raft.Peer

	storage   *raft.MemoryStorage
	transport t.Transport
	node      raft.Node

	idGen *idutil.Generator
	w     wait.Wait

	done chan struct{}

	log *zap.Logger
}

func NewRaftNode(id int, peers []uint64) *raftNode {
	raftPeers := make([]raft.Peer, len(peers))

	for i := 0; i < len(peers); i++ {
		raftPeers[i] = raft.Peer{ID: peers[i]}
	}

	logger, _ := zap.NewDevelopment()

	rn := &raftNode{
		id:    uint64(id),
		peers: raftPeers,

		idGen: idutil.NewGenerator(uint16(id), time.Now()),
		w:     wait.New(),

		log:  logger,
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
	case <-ch:
		return nil
	case <-ctx.Done():
		rn.w.Trigger(cc.ID, nil)
		return ctx.Err()
	case <-rn.done:
		return nil
	}
}

func (rn *raftNode) AddMember(id uint64) {
	err := rn.configure(context.TODO(), raftpb.ConfChange{
		Type:   raftpb.ConfChangeAddNode,
		NodeID: id,
	})

	rn.log.Info("Added member",
		zap.Uint64("id", rn.id),
		zap.Uint64("added_id", id),
		zap.Error(err),
	)
}

func (rn *raftNode) RemoveMember(id uint64) {
	err := rn.configure(context.TODO(), raftpb.ConfChange{
		Type:   raftpb.ConfChangeRemoveNode,
		NodeID: id,
	})

	rn.log.Info("Removed member",
		zap.Uint64("id", rn.id),
		zap.Uint64("removed_id", id),
		zap.Error(err),
	)
}

func (rn *raftNode) Propose(val string) {
	rn.node.Propose(context.TODO(), []byte(val))
}

}

func (rn *raftNode) raftLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)

	for {
		select {
		case <-ticker.C:
			rn.node.Tick()
		case rd := <-rn.node.Ready():
			rn.saveToStorage(rd.HardState, rd.Entries, rd.Snapshot)
			rn.transport.Send(rd.Messages)

			if !raft.IsEmptySnap(rd.Snapshot) {
				rn.processSnapshot(rd.Snapshot)
			}

			for _, entry := range rd.CommittedEntries {
				rn.process(entry)
				if entry.Type == raftpb.EntryConfChange {
					var cc raftpb.ConfChange
					cc.Unmarshal(entry.Data)

					rn.applyConfChange(cc)
					rn.w.Trigger(cc.ID, struct{}{})
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
	rn.node.ApplyConfChange(cc)

	switch cc.Type {
	case raftpb.ConfChangeAddNode:
		rn.log.Info("Add node",
			zap.Uint64("id", rn.id),
			zap.Uint64("added_id", cc.NodeID),
		)

		rn.transport.AddPeer(rn)
	case raftpb.ConfChangeRemoveNode:
		rn.log.Info("Remove node",
			zap.Uint64("id", rn.id),
			zap.Uint64("removed_id", cc.NodeID),
		)
		if cc.NodeID == rn.id {
			rn.log.Info("Removed self", zap.Uint64("id", rn.id))
			rn.done <- struct{}{}
			rn.transport.RemovePeer(rn)
		}
	}
	return nil
}

func (rn *raftNode) process(entry raftpb.Entry) {
	// TODO: add logic
	rn.log.Info("Raft entry",
		zap.Uint64("id", rn.id),
		zap.Stringer("entry", &entry),
	)
}

func (rn *raftNode) processSnapshot(snap raftpb.Snapshot) {
	rn.storage.ApplySnapshot(snap)
}

func (rn *raftNode) Id() uint64 {
	return rn.id
}

func (rn *raftNode) Process(ctx context.Context, msg raftpb.Message) error {
	return rn.node.Step(ctx, msg)
}
