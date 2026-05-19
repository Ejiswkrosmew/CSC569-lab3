package main

import (
	"fmt"
	"lab3/shared"
	"io/ioutil"
	"sort"
	"math/rand"
	"net/rpc"
	"os"
	"sync"
	"time"
	"unicode"
	"strings"
	"strconv"
)

const (
	MAX_NODES     = 4
	X_TIME        = 1
	Y_TIME        = 2
	Z_TIME_MAX    = 100
	Z_TIME_MIN    = 10
	T_FAIL        = 10
	T_CLEAN       = 2 * T_FAIL
	RAFT_DELAY    = 5 * Y_TIME
	CAND_TIME_MIN = 150 // ms
	CAND_TIME_MAX = 300 // ms
	RAFT_HB       = 50  // ms
)


var isWorking = false

var start time.Time = time.Now()
var self_node shared.Node
var self_RAFT_node *shared.RAFTNode //need to make this a pointer so we can pass it around
var election_timeout *time.Timer


func broadcastRAFT(server *rpc.Client, req shared.RAFTRequest, membership *shared.Membership) {
	for _, receiver := range (*membership).Keys() {
		if receiver == req.From {
			continue
		}
		newReq := req
		newReq.To = receiver //send to this reciever
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
	self_RAFT_node = &shared.RAFTNode{
		State: 0,
		Term: 0,
		Vote: 0,
		Votes: 0,
		Log: []shared.LogEntry{},
		CommitIndex: -1,
		LastApplied: -1,
		}
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
	//time.AfterFunc(time.Second*time.Duration(Z_TIME), func() { runAfterZ(server, id) })

	// Delaying the election until the membership table is probably filled
	fmt.Printf("Waiting %d seconds for the membership list to fill out...\n", RAFT_DELAY)
	election_timeout = time.AfterFunc(time.Second*RAFT_DELAY+time.Millisecond*time.Duration(rand.Float32()*(CAND_TIME_MAX-CAND_TIME_MIN)+CAND_TIME_MIN), func() { runElectionLoop(server, self_RAFT_node, &membership, id, &election_timeout) })
	time.AfterFunc(time.Millisecond*RAFT_HB, func() { runRAFTHB(server, self_RAFT_node, &membership, id, &election_timeout) })

	//dedicated worker loop
	time.AfterFunc(time.Second*RAFT_DELAY, func() { runWorkerExecutionLoop(server, id) })

	wg.Add(1)
	wg.Wait()
}

func runElectionLoop(server *rpc.Client, node *shared.RAFTNode, membership **shared.Membership, id int, election_timeout **time.Timer) {
	//Stop the old timer if it exists before assigning a new one
	if *election_timeout != nil {
		(*election_timeout).Stop()
	}

	// Schedule the next potential election timeout window safely
	*election_timeout = time.AfterFunc(time.Millisecond*time.Duration(rand.Float32()*(CAND_TIME_MAX-CAND_TIME_MIN)+CAND_TIME_MIN), func() { 
		runElectionLoop(server, node, membership, id, election_timeout) 
	})

	// New term increment
	node.Term++

	// Now a candidate
	node.State = 1
	node.Vote = id     //A candidate always votes for itself first!
	node.Votes = 1     //Start with 1 vote (your own)

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
			node.Leader = 0
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
		case 2: // Leader Ping
			if node.Leader != req.From {
				fmt.Printf("NODE %d (%.2fs): Leader %d acknowledged\n", id, time.Since(start).Seconds(), req.From)
			}

			node.State = 0
			node.Leader = req.From

			(*election_timeout).Reset(time.Millisecond * time.Duration(rand.Float32()*(CAND_TIME_MAX-CAND_TIME_MIN)+CAND_TIME_MIN))
		case 3: //follower replication confirmation
			if node.State != 2 {
				continue //dont care if Im not the leader
			}

			if req.Success {
				node.MatchIndex[req.From] = req.PrevLogIndex
				node.NextIndex[req.From] = req.PrevLogIndex + 1
				for N := len(node.Log) - 1; N > node.CommitIndex; N-- {
					// We must only commit logs generated during our active term
					if node.Log[N].Term == node.Term {
						count := 1 // Start at 1 to count ourselves (the leader)
						for _, peerID := range (*membership).Keys() {
							if peerID != id && node.MatchIndex[peerID] >= N {
								count++ //cont it if its not us and it is caught up to here
							}
						}
						
						// If a  majority of the cluster confirmed it, commit it!
						if count >= (*membership).Len()/2+1 {
							node.CommitIndex = N
							fmt.Printf("LEADER %d: Majority consensus reached. Committed up to log index %d\n", id, node.CommitIndex)
							break
						}
					}
				}

			} else {
				//follower rejected, back up to next most recent
				//this will happen until we reach consensus
				if node.NextIndex[req.From] > 0 {
					node.NextIndex[req.From]--
				}
			}
		}
	}

	// Check if, as a candidate, majority votes were received
	if node.State == 1 && node.Votes >= (*membership).Len()/2+1 {
		// If majority, now a leader
		node.State = 2
		(*election_timeout).Stop()
		fmt.Printf("NODE %d (%.2fs): ELECTED AS LEADER\n", id, time.Since(start).Seconds())

		//init log stuff
		node.NextIndex = make(map[int]int)
		node.MatchIndex = make(map[int]int)
		for _, peerID := range (*membership).Keys() {
			node.NextIndex[peerID] = len(node.Log) // Default to leader's current log length
			node.MatchIndex[peerID] = -1           // Peer starts with no confirmed matches
		}

		// check for dead workers, set those tasks to not started
		go func() {
			for {
				time.Sleep(2 * time.Second)
				shared.MRMutex.Lock()
				now := time.Now()
				for key, startTime := range shared.TaskTimestamps {
					if now.Sub(startTime) > 10*time.Second { // 10-second timeout
						var taskID int
						if strings.HasPrefix(key, "map-") {
							fmt.Sscanf(key, "map-%d", &taskID)
							shared.MapTasks[taskID] = 0 // wipe progress set to idle
						} else {
							fmt.Sscanf(key, "reduce-%d", &taskID)
							shared.ReduceTasks[taskID] = 0 // wipe progress reset to idle
						}
						delete(shared.TaskTimestamps, key) //remove task
					}
				}
				shared.MRMutex.Unlock()
			}
		}()
	}

	// Broadcast leader ping to all nodes if leader
	if node.State == 2 {
		var masterServerLog []shared.LogEntry
		err := server.Call("Coordinator.GetLog", id, &masterServerLog)
		if err == nil {
			node.Log = masterServerLog //make our log the most recent master log
		}

		//compute log slices for each follower
		for _, receiver := range (*membership).Keys() {
			if receiver == id {
				continue //skip me, Im the leader
			}

			// Look up what log entry immediately precedes the slice we are sending
			pIdx := node.NextIndex[receiver] - 1
			pTerm := -1
			if pIdx >= 0 && pIdx < len(node.Log) {
				pTerm = node.Log[pIdx].Term
			}

			// Slice the exact log subset that this specific follower is missing
			var entriesToSend []shared.LogEntry
			if node.NextIndex[receiver] >= 0 && node.NextIndex[receiver] < len(node.Log) {
				entriesToSend = node.Log[node.NextIndex[receiver]:]
			} else if node.NextIndex[receiver] == len(node.Log) {
				entriesToSend = []shared.LogEntry{} // Follower caught up send empty
			}

			// Package the complete, consistency-checked payload frame
			req := shared.RAFTRequest{
				To:           receiver,
				From:         id,
				Term:         node.Term,
				Type:         2, // AppendEntries Frame
				PrevLogIndex: pIdx,
				PrevLogTerm:  pTerm,
				Entries:      entriesToSend,
				LeaderCommit: node.CommitIndex,
			}

			// Fire the packet asynchronously over our network wrapper
			var reply bool
			go server.Call("RAFTRequests.Add", req, &reply)
		}
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

//actual map and reduce funcs
func Map(filename string, contents string) []KeyValue {
	// Function to detect word separators (returns true if the character is NOT a letter)
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	// Split contents into an array of clean words based on our separator function
	words := strings.FieldsFunc(contents, ff)

	kva := []KeyValue{}
	for _, w := range words {
		// Create a key-value pair for each word found
		kv := KeyValue{Key: w, Value: "1"}
		kva = append(kva, kv)
	}
	return kva
}

func Reduce(key string, values []string) string {
	// Return the total number of occurrences of this word as a string
	return strconv.Itoa(len(values))
}

//assign buckets
func getBucket(word string, nReduce int) int {
	sum := 0
	for _, r := range word {
		sum += int(r)
	}
	return sum % nReduce
}

func executeSimpleMap(taskID int, filename string, nReduce int, server *rpc.Client) {
	// 1. Open and read the raw content of the input file
	file, err := os.Open(filename)
	if err != nil {
		fmt.Printf("Worker Error: Cannot open file %v\n", filename)
		return
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		fmt.Printf("Worker Error: Cannot read file %v\n", filename)
		file.Close()
		return
	}
	file.Close()

	// 2. Process contents into a slice of individual words paired with "1"
	kva := Map(filename, string(content))

	// 3. Open file handles for each of our destination partition buckets
	files := make([]*os.File, nReduce)
	for b := 0; b < nReduce; b++ {
		outName := fmt.Sprintf("mr-%d-%d", taskID, b)
		f, err := os.Create(outName)
		if err != nil {
			fmt.Printf("Worker Error: Cannot create partition file %s\n", outName)
			return
		}
		files[b] = f
	}

	// 4. Distribute each KeyValue pair into its calculated bucket file
	for _, kv := range kva {
		b := getBucket(kv.Key, nReduce)
		// Write out as clear plain text lines: "word 1"
		fmt.Fprintf(files[b], "%s %s\n", kv.Key, kv.Value)
	}

	// Close all partition files to flush the data to disk
	for b := 0; b < nReduce; b++ {
		files[b].Close()
	}

	// 5. Notify the RAFT Leader that this Map task successfully completed
	var reply bool
	args := shared.CompleteTaskArgs{TaskType: 1, TaskID: taskID}
	server.Call("Coordinator.CompleteTask", &args, &reply)
}

func executeSimpleReduce(taskID int, nMap int, server *rpc.Client) {
	intermediate := []KeyValue{}

	// 1. Collect intermediate files from all Map tasks for this bucket partition ID
	for m := 0; m < nMap; m++ {
		inName := fmt.Sprintf("mr-%d-%d", m, taskID)
		file, err := os.Open(inName)
		if err != nil {
			continue // Safe to skip if a Map task didn't produce keys for this specific bucket
		}

		var key, val string
		// Read lines sequentially
		for {
			_, err := fmt.Fscanf(file, "%s %s\n", &key, &val)
			if err != nil {
				break // Hit EOF, break out to stop reading this specific file
			}
			intermediate = append(intermediate, KeyValue{Key: key, Value: val})
		}
		file.Close()
	}

	// 2. Sort the collected slice so identical words are forced into adjacent positions
	sort.Sort(ByKey(intermediate))

	// 3. Open the final aggregated word count file
	outName := fmt.Sprintf("mr-out-%d", taskID)
	ofile, err := os.Create(outName)
	if err != nil {
		fmt.Printf("Worker Error: Cannot create output file %s\n", outName)
		return
	}

	// 4. Process identical key blocks sequentially using a two-pointer sliding window
	i := 0
	for i < len(intermediate) {
		j := i + 1
		// Advance 'j' as long as the sequential key tokens match perfectly
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		
		// Collect all the value strings (which will just be strings of "1")
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		
		// Execute the Reduce function to count the size of the value slice
		output := Reduce(intermediate[i].Key, values)

		// Print the final result formatted to disk: "word count"
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		// Advance your index anchor over to the next unique word sequence
		i = j
	}
	ofile.Close()

	// 5. Notify the RAFT Leader that this Reduce task successfully completed
	var reply bool
	args := shared.CompleteTaskArgs{TaskType: 2, TaskID: taskID}
	server.Call("Coordinator.CompleteTask", &args, &reply)
}

// Intermediate data structure
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int          { return len(a) } 
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


func runWorkerExecutionLoop(server *rpc.Client, id int) {
	// Re-queue this function to evaluate every 500ms
	time.AfterFunc(500*time.Millisecond, func() { runWorkerExecutionLoop(server, id) })

	// Only request tasks if I am a healthy follower, I know who the leader is, and I am not busy
	if self_RAFT_node.State == 0 && self_RAFT_node.Leader != 0 && !isWorking {
		isWorking = true
		
		go func() {
			var reply shared.TaskReply
			args := shared.TaskRequest{WorkerID: id}
			
			err := server.Call("Coordinator.GiveOutTask", &args, &reply)
			if err == nil {
				switch reply.TaskType {
				case 1: // Map Task
					fmt.Printf("NODE %d: Received Map Task %d (%s)\n", id, reply.TaskID, reply.Filename)
					executeSimpleMap(reply.TaskID, reply.Filename, reply.NReduce, server)
					isWorking = false // Task finished, reset flag
				case 2: // Reduce Task
					fmt.Printf("NODE %d: Received Reduce Task %d\n", id, reply.TaskID)
					executeSimpleReduce(reply.TaskID, reply.NMap, server)
					isWorking = false // Task finished, reset flag
				default:
					// Type 0 (Wait) or Type 3 (All Done)
					isWorking = false
				}
			} else {
				// Server error or connection dropped temporarily
				isWorking = false
			}
		}()
	}
}
