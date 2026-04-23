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
	MAX_NODES  = 8
	X_TIME     = 1
	Y_TIME     = 2
	Z_TIME_MAX = 100
	Z_TIME_MIN = 10
	T_FAIL     = 10
	T_CLEAN    = 2 * T_FAIL
)

var start time.Time = time.Now()
var self_node shared.Node

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

	wg.Add(1)
	wg.Wait()
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
	currTime := calcTime()
	for key, node := range (*membership).Members {
		if currTime-node.Time > T_CLEAN {
			delete((*membership).Members, key)
		} else if currTime-node.Time > T_FAIL {
			node.Alive = false
			(*membership).Members[key] = node
		}
	}
}

func runAfterY(server *rpc.Client, neighbors [2]int, membership **shared.Membership, id int) {
	// Queue the next loop
	time.AfterFunc(time.Second*Y_TIME, func() { runAfterY(server, neighbors, membership, id) })

	neighbor := neighbors[rand.Intn(2)]
	// fmt.Printf("Neighbor %d was chosen\n", neighbor)
	sendMessage(server, neighbor, *membership)

	(*membership).Print()
}

func runAfterZ(server *rpc.Client, id int) {
	fmt.Printf("NODE %d FAILED\n", id)
	os.Exit(1)
}
