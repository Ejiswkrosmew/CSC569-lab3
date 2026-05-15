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

func initiatePaxos(server *rpc.Client, membership *shared.Membership, myProposalNum int, proposedValue shared.PaxosValue) {
	time_start := time.Now()
	membership.Lock() //had to make setters for this to access from client
	// create array of IDs to look through statically
	//looking through the membership list this whole time
	//would lock up the list
	targetIDs := []int{}
	for id := range membership.Members {
		targetIDs = append(targetIDs, id)
	}
	membership.Unlock()

	//define majoroity
	numNodes := len(targetIDs)
	majority := (numNodes / 2) + 1
	
	// prepare
	fmt.Printf("\n[Paxos] Starting Prepare with ID %d\n", myProposalNum)
	
	promisesReceived := 0
	valueToPropose := proposedValue

	//send a prepare to every client in node list, count promises
	for _, targetID := range targetIDs {
		var reply shared.PromiseResponse //promise to be filled out by target
		req := shared.PrepareRequest{ProposalNum: myProposalNum, TargetID: targetID} //prepare request being sent
		
		err := server.Call("PaxosManager.Prepare", req, &reply)
		if err == nil && reply.Promise { //if we got a promise back for the given node increment counter
			promisesReceived++
		}
	}

	//did we get enough promises
	if promisesReceived < majority {
		fmt.Printf("[Paxos] Failed to reach majority, exiting paxos\n")
		return
	}

	fmt.Printf("[Paxos] %d of %d required accepted. Value to propose: %s\n", promisesReceived, majority, valueToPropose)
	// accept phase
	fmt.Printf("[Paxos] Starting accept phase for value: %s\n", valueToPropose)
	
	acceptsReceived := 0
	//send accept request to all nodes
	for _, targetID := range targetIDs {
		var accepted bool
		req := shared.AcceptRequest{
			ProposalNum: myProposalNum, 
			Value: valueToPropose,
			TargetID: targetID,
		}
		
		err := server.Call("PaxosManager.Accept", req, &accepted) //do accept request logic on each client state
		if err == nil && accepted {
			acceptsReceived++ 
		}
	}

	if acceptsReceived >= majority {
		fmt.Printf("[Paxos] success. Value '%s' learned by majority (%d/%d).\n\n", valueToPropose, acceptsReceived, majority)
	} else {
		fmt.Printf("[Paxos] Consensus failed.\n")
	}
	fmt.Printf("time taken to reach consensus: %f",  time.Since(time_start).Seconds())
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

	// initiate with keypress
    go func() {
		//infinite loop allow preparer to initiare on key press in the background
        for {
            fmt.Println("\n--- Press enter to initiate Paxos ---")
            fmt.Scanln() 
			//seed proposal number from timestamp and node id
            proposalID := int(time.Now().Unix()%10000)*100 + id
			//set value being proposed
            val := shared.PaxosValue(fmt.Sprintf("Value-from-Node-%d", id))
            initiatePaxos(server, membership, proposalID, val)
        }
    }()

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
