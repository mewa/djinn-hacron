package raft

type Handler struct {
	Process      func(data []byte)
	UpdateLeader func(newLeader uint64)
}
