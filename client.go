package main

import (
	"fmt"
	"lab3/shared"
	"math/rand"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	MAX_NODES     = 8
	X_TIME        = 1
	Y_TIME        = 2
	Z_TIME_MAX    = 100
	Z_TIME_MIN    = 10
	T_FAIL        = 10
	T_CLEAN       = 2 * T_FAIL
	CAND_TIME_MIN = 150 // ms
	CAND_TIME_MAX = 300 // ms
	RAFT_HB       = 50  // ms
)

var start time.Time = time.Now()
var self_node shared.Node
var self_RAFT_node shared.RAFTNode
var election_timeout *time.Timer

func broadcastRAFT(server *rpc.Client, req shared.RAFTRequest, membership *shared.Membership) {
	for _, receiver := range (*membership).Keys() {
		if receiver == req.From {
			continue
		}
		newReq := shared.RAFTRequest{
			To:   receiver,
			From: req.From,
			Term: req.Term,
			Type: req.Type,
		}
		var reply bool
		go server.Call("RAFTRequests.Add", newReq, &reply)
	}
}

// Send the current membership table to a neighboring node with the provided ID
func sendMessage(server *rpc.Client, id int, membership *shared.Membership) {
	req := shared.Request{ID: id, Table: membership}
	var reply bool
	// fmt.Print("Attempting to send message to server\n")
	server.Call("Requests.Add", req, &reply)
	// fmt.Print("Message sent\n")
}

// Read incoming messages from other nodes
func readMessages(server *rpc.Client, id int, membership *shared.Membership) *shared.Membership {
	var reply shared.Membership
	// fmt.Print("Attempting to read messages from server\n")
	server.Call("Requests.Listen", id, &reply)
	// fmt.Print("Message received\n")

	ret := shared.CombineTables(membership, shared.NewMembership())

	currTime := calcTime()
	for key := range reply.Members {
		var node, old shared.Node
		reply.Get(key, &node)

		if membership.Get(key, &old) == nil {
			// Potential update
			if old.Hbcounter < node.Hbcounter {
				// Actually updating
				node.Time = currTime
				ret.Update(node, &node)
			}
		} else {
			// New being added
			node.Time = currTime
			ret.Add(node, &node)
		}
	}

	return ret
}

func calcTime() float64 {
	// return float64(time.Now().UnixMilli()) / 1000
	return time.Since(start).Seconds()
}

var wg = &sync.WaitGroup{}

func main() {
	rand.Seed(time.Now().UnixNano())
	Z_TIME := rand.Intn(Z_TIME_MAX-Z_TIME_MIN) + Z_TIME_MIN

	// Connect to RPC server
	server, _ := rpc.DialHTTP("tcp", "localhost:9005")

	args := os.Args[1:]

	// Get ID from command line argument
	if len(args) == 0 {
		fmt.Println("No args given")
		return
	}
	id, err := strconv.Atoi(args[0])
	if err != nil {
		fmt.Println("Found Error", err)
	}

	fmt.Println("Node", id, "will fail after", Z_TIME, "seconds")

	currTime := calcTime()
	// Construct self
	self_node = shared.Node{ID: id, Hbcounter: 0, Time: currTime, Alive: true}
	self_RAFT_node = shared.RAFTNode{State: 0, Term: 0, Vote: 0, Votes: 0}
	var self_node_response shared.Node // Allocate space for a response to overwrite this

	// Add node with input ID
	if err := server.Call("Membership.Add", self_node, &self_node_response); err != nil {
		fmt.Println("Error:2 Membership.Add()", err)
	} else {
		fmt.Printf("Success: Node created with id= %d\n", id)
	}

	neighbors := self_node.InitializeNeighbors(id)
	fmt.Println("Neighbors:", neighbors)

	membership := shared.NewMembership()
	membership.Add(self_node, &self_node)

	sendMessage(server, neighbors[0], membership)

	// crashTime := self_node.CrashTime()

	time.AfterFunc(time.Second*X_TIME, func() { runAfterX(server, &self_node, &membership, id) })
	time.AfterFunc(time.Second*Y_TIME, func() { runAfterY(server, neighbors, &membership, id) })
	time.AfterFunc(time.Second*time.Duration(Z_TIME), func() { runAfterZ(server, id) })

	// Delaying the election until the membership table is probably filled
	fmt.Printf("Waiting %d seconds for the membership list to fill out...\n", Y_TIME*3)
	election_timeout = time.AfterFunc(time.Second*Y_TIME*3+time.Millisecond*time.Duration(rand.Float32()*(CAND_TIME_MAX-CAND_TIME_MIN)+CAND_TIME_MIN), func() { runElectionLoop(server, &self_RAFT_node, &membership, id, &election_timeout) })
	time.AfterFunc(time.Second*Y_TIME*3+time.Millisecond*RAFT_HB, func() { runRAFTHB(server, &self_RAFT_node, &membership, id, &election_timeout) })

	wg.Add(1)
	wg.Wait()
}

func runElectionLoop(server *rpc.Client, node *shared.RAFTNode, membership **shared.Membership, id int, election_timeout **time.Timer) {
	*election_timeout = time.AfterFunc(time.Millisecond*time.Duration(rand.Float32()*(CAND_TIME_MAX-CAND_TIME_MIN)+CAND_TIME_MIN), func() { runElectionLoop(server, node, membership, id, election_timeout) })

	// New term
	node.Term++

	// Now a candidate
	node.State = 1

	// Broadcast vote request
	req := shared.RAFTRequest{
		From: id,
		Term: node.Term,
		Type: 0,
	}
	broadcastRAFT(server, req, *membership)

	fmt.Printf("\nNODE %d (%.2fs): Leader ping timed out. Now becoming a candidate for term %d\n", id, time.Since(start).Seconds(), node.Term)
}

func runRAFTHB(server *rpc.Client, node *shared.RAFTNode, membership **shared.Membership, id int, election_timeout **time.Timer) {
	time.AfterFunc(time.Millisecond*RAFT_HB, func() { runRAFTHB(server, node, membership, id, election_timeout) })

	var reply []shared.RAFTRequest
	server.Call("RAFTRequests.Listen", id, &reply)

	for _, req := range reply {
		// If a new term is decalred, self now starts as a follower
		if req.Term > node.Term {
			node.Term = req.Term
			node.State = 0
			node.Vote = 0
			node.Votes = 0
			fmt.Printf("\nNODE %d (%.2fs): New term received (%d)\n", id, time.Since(start).Seconds(), node.Term)
		}

		// If an old term, ignore
		if req.Term < node.Term {
			continue
		}

		switch req.Type {
		case 0: // Requesting Vote
			if node.State != 0 || node.Vote != 0 {
				// No-op if not a follower or already promised a vote
				continue
			}

			// Vote for requester and reset the timeout
			node.Vote = req.From
			response := shared.RAFTRequest{
				To:   req.From,
				From: id,
				Term: node.Term,
				Type: 1,
			}
			var reply bool
			server.Call("RAFTRequests.Add", response, &reply)
			(*election_timeout).Reset(time.Millisecond * time.Duration(rand.Float32()*(CAND_TIME_MAX-CAND_TIME_MIN)+CAND_TIME_MIN))

			fmt.Printf("NODE %d (%.2fs): Now voting for %d\n", id, time.Since(start).Seconds(), req.From)
		case 1: // Promised Vote
			if node.State != 1 {
				// No-op if not a candidate
				continue
			}

			// Vote received
			node.Votes++
			fmt.Printf("NODE %d (%.2fs): Received a vote from %d (votes: %d/%d)\n", id, time.Since(start).Seconds(), req.From, node.Votes, (*membership).Len())

			if node.Votes > (*membership).Len()/2 {
				// If majority, now a leader
				node.State = 2
				(*election_timeout).Stop()
				fmt.Printf("\tMajority votes received. Now a leader\n")
			}
		case 2: // Leader Ping
			node.State = 0
			node.Vote = req.From
			// fmt.Printf("NODE %d (%.2fs): Leader ping from %d received\n", time.Since(start).Seconds(), req.From)
			(*election_timeout).Reset(time.Millisecond * time.Duration(rand.Float32()*(CAND_TIME_MAX-CAND_TIME_MIN)+CAND_TIME_MIN))
		}
	}

	// Broadcast leader ping to all nodes if leader
	if node.State == 2 {
		req := shared.RAFTRequest{
			From: id,
			Term: node.Term,
			Type: 2,
		}
		broadcastRAFT(server, req, *membership)
		// fmt.Printf("NODE %d (%.2fs): Leader Broadcast performed\n", id, time.Since(start).Seconds(), req.From)
	}
}

func runAfterX(server *rpc.Client, node *shared.Node, membership **shared.Membership, id int) {
	// Queue the next loop
	time.AfterFunc(time.Second*X_TIME, func() { runAfterX(server, node, membership, id) })

	node.Hbcounter++
	node.Time = calcTime()
	server.Call("Membership.Update", *node, node)
	(*membership).Update(*node, node)

	// Check for new messeges
	newMem := readMessages(server, id, *membership)
	*membership = shared.CombineTables(*membership, newMem)

	// Dead checker
	(*membership).UpdateDead(calcTime(), T_FAIL, T_CLEAN)
}

func runAfterY(server *rpc.Client, neighbors [2]int, membership **shared.Membership, id int) {
	// Queue the next loop
	time.AfterFunc(time.Second*Y_TIME, func() { runAfterY(server, neighbors, membership, id) })

	neighbor := neighbors[rand.Intn(2)]
	// fmt.Printf("Neighbor %d was chosen\n", neighbor)
	sendMessage(server, neighbor, *membership)

	// (*membership).Print()
}

func runAfterZ(server *rpc.Client, id int) {
	fmt.Printf("NODE %d FAILED\n", id)
	os.Exit(1)
}
