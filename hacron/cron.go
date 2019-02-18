package hacron

import (
	"github.com/robfig/cron"
	"github.com/mewa/djinn/raft"
	"time"
)

type Djinn struct {
	c *cron.Cron
	raft *raft.RaftNode
}

func NewDjinn(peers []string) *Djinn {
	hb := time.Duration(100)

	djinn := &Djinn{
		c: cron.New(),
	}
	djinn.raft = raft.NewRaftNode(1, peers, hb * time.Millisecond, &raft.Handler{
		Process: func(data []byte) {
			djinn.Process(nil)
		},
		UpdateLeader: djinn.updateLeader,
	})

	return djinn
}

func (d *Djinn) Process(data []byte) {
	
}

func (d *Djinn) updateLeader(leader uint64) {
	
}
