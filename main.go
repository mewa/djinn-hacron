package main

import (
//	"github.com/mewa/djinn/hacron"
	"github.com/mewa/djinn/raft"
	"time"
//	"context"
	"fmt"
)

// func main() {
// 	peers := []string{"http://djinn1"}
// 	d1 := hacron.NewDjinn(peers[0], peers)
// 	d2 := hacron.NewDjinn("http://djinn2", peers)
// }

func main2() {
	peers := []string{"http://djinn1"}

	//ctx, _ := context.WithTimeout(context.Background(), time.Second * 10)
	hb := time.Duration(100)
	node := 
		raft.NewRaftNode(1, peers, hb * time.Millisecond)
	
	node2 := 
		raft.NewRaftNode(2, nil, hb * time.Millisecond)

	node3 :=
		raft.NewRaftNode(3, nil, hb * time.Millisecond)

	node.AddMember(2, "http://djinn2")


	err := node.Write("2222222222222")
	if err != nil {
		fmt.Printf("%#v\n", err)
		panic(err)
	}
	err = node.Write("1111111111111")
	if err != nil { panic(err) }

	fmt.Println("========== ADD 3 ==========")

	err = node2.AddMember(3, "http://djinn3")
//	if err != nil { panic(err) }
	err = node.AddMember(3, "http://djinn3")
//	if err != nil { panic(err) }
//	node2.RemoveMember(1)

	fmt.Println("========== REMOVED ==========")

	//	time.Sleep(5 * time.Second)

	//lbl:
	//err = node2.Write("FINAL_22")
	// if err != nil {
	// 	//goto lbl
	// 	panic(err)
	// }

	err = node3.Write("FINAL_33")
	if err != nil { panic(err) }


	time.Sleep(1 * time.Second)

	node = 
		raft.NewRaftNode(4, nil, hb * time.Millisecond)
	err = node3.AddMember(4, "http://djinn1")
	if err != nil { panic(err) }

	time.Sleep(time.Second)

	//node2.RemoveMember(1)

	node2.Write("xxxx the end")

	//time.Sleep(time.Second)

	node.PrintData()
	node2.PrintData()
	node3.PrintData()

	time.Sleep(time.Second * 5)
	return
	var nextPeer int = 4
	rmNode := node2
	lastNode := node3
	for i := 0; ; i++ {
		node = raft.NewRaftNode(nextPeer, nil, hb * time.Millisecond)
		
		lastNode.AddMember(uint64(nextPeer), fmt.Sprintf("http://djinn%d", nextPeer % 3 + 1))

		if false {rmNode.RemoveMember(uint64(nextPeer - 2))}
		rmNode = lastNode
		lastNode = node

		nextPeer++
		
		err = node.Propose(fmt.Sprintf("%s=======%d", "the end", i))
		if err != nil { panic(err) }
	}
}
