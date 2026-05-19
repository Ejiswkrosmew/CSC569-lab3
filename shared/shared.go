package shared

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

const (
	MAX_NODES = 4
)

//log entry
type LogEntry struct {
	Command string //"MapTask-0-Completed"
	Term    int    // The term this entry was created in
}

//-- map reduce stuff

var InputFiles = []string{"pg-being_ernest.txt", "pg-metamorphosis.txt"}
var NReduce = 3

// Task tracking: 0 = Idle, 1 = In Progress, 2 = Completed
var MapTasks []int
var ReduceTasks []int
var TaskTimestamps map[string]time.Time

var MRMutex sync.Mutex

type Coordinator struct {
	Log         []LogEntry
	CommitIndex int
}

type TaskRequest struct {
	WorkerID int
	Term int
}

type TaskReply struct {
	TaskType int    // 0 = Wait, 1 = Map, 2 = Reduce, 3 = All Done
	TaskID   int    
	Filename string
	NReduce  int   
	NMap     int   
}

type CompleteTaskArgs struct {
	TaskType int // 1 = Map, 2 = Reduce
	TaskID   int
	Term int
}


type RAFTNode struct {
	State  int // 0: follower, 1: candidate, 2: leader
	Term   int
	Vote   int
	Votes  int
	Leader int

	//log stuff
	Log          []LogEntry // local log
	CommitIndex  int        // highest log entry committed
	LastApplied  int        // highest log entry applied
	
	// Leader stuff
	NextIndex    map[int]int //	index of the next log entry to send
	MatchIndex   map[int]int // index of highest log entry known to be replicated
}

/*---------------*/

// Node struct represents a computing node.
type Node struct {
	ID        int
	Hbcounter int
	Time      float64
	Alive     bool
}

// Generate random crash time from 10-60 seconds
func (n Node) CrashTime() int {
	rand.Seed(time.Now().UnixNano())
	max := 60
	min := 10
	return rand.Intn(max-min) + min
}

func (n Node) InitializeNeighbors(id int) [2]int {
	//for mapreduce I force 1-4 to know their neighbors
	
	// Find neighbor ahead
	next := id + 1
	if next > 4 {
		next = 1
	}

	// Find neighbor behind
	prev := id - 1
	if prev < 1 {
		prev = 4
	}

	return [2]int{next, prev}
}

func RandInt() int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(MAX_NODES-1+1) + 1
}

/*---------------*/

// Membership struct represents participanting nodes
type Membership struct {
	mu      sync.Mutex // JY: Added mutex for synchronization
	Members map[int]Node
}

// Returns a new instance of a Membership (pointer).
func NewMembership() *Membership {
	return &Membership{
		Members: make(map[int]Node),
	}
}

// Adds a node to the membership list.
func (m *Membership) Add(payload Node, reply *Node) error {
	// Safely set value
	m.mu.Lock()
	// fmt.Pring("Membership Lock (Add)\n")
	m.Members[payload.ID] = payload
	m.mu.Unlock()
	// fmt.Pring("Membership Unock (Add)\n")

	// Set reply to added node
	*reply = payload

	// Return success
	return nil
}

// Updates a node in the membership list.
func (m *Membership) Update(payload Node, reply *Node) error {
	// Safely set value
	m.mu.Lock()
	// fmt.Pring("Membership Lock (Update)\n")
	m.Members[payload.ID] = payload
	m.mu.Unlock()
	// fmt.Pring("Membership UnLock (Update)\n")

	// Set reply to updated node
	*reply = payload

	// Return success
	return nil
}

// Returns a node with specific ID.
func (m *Membership) Get(payload int, reply *Node) error {
	// Safely get val
	m.mu.Lock()
	// fmt.Pring("Membership Lock (Get)\n")
	temp, ok := m.Members[payload]

	// Ensure to unlock upon return
	// defer fmt.Pring("Membership Unlock (Get)\n")
	defer m.mu.Unlock()

	// If couldn't be found, return error
	if !ok {
		return errors.New("Node not found")
	}

	// Store val in reply
	*reply = temp

	// Return success
	return nil
}

func (m *Membership) UpdateDead(currTime float64, t_fail float64, t_dead float64) {
	m.mu.Lock()
	for key, node := range m.Members {
		if currTime-node.Time > t_dead {
			delete(m.Members, key)
		} else if currTime-node.Time > t_fail {
			node.Alive = false
			m.Members[key] = node
		}
	}
	m.mu.Unlock()
}

func (m *Membership) Len() int {
	m.mu.Lock()
	ret := len(m.Members)
	m.mu.Unlock()

	return ret
}

func (m *Membership) Keys() []int {
	m.mu.Lock()
	ret := make([]int, len(m.Members))
	i := 0
	for k := range m.Members {
		ret[i] = k
		i++
	}
	m.mu.Unlock()
	return ret
}

func (m *Membership) Print() {
	m.mu.Lock()
	for _, val := range m.Members {
		status := "is Alive"
		if !val.Alive {
			status = "is Dead"
		}
		fmt.Printf("Node %d has hb %d, time %.1f and %s\n", val.ID, val.Hbcounter, val.Time, status)
	}
	fmt.Println("")
	m.mu.Unlock()
}

/*---------------*/

type RAFTRequest struct {
	To   int
	From int
	Term int
	Type int // 0: request, 1: vote, 2: leader-ping

	//log replication
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
	Success      bool       // followers reply back
}

type RAFTRequests struct {
	mu      sync.Mutex
	Pending map[int][]RAFTRequest
}

func NewRAFTRequests() *RAFTRequests {
	return &RAFTRequests{
		Pending: make(map[int][]RAFTRequest),
	}
}

func (r *RAFTRequests) Add(payload RAFTRequest, reply *bool) error {
	r.mu.Lock()
	r.Pending[payload.To] = append(r.Pending[payload.To], payload)
	r.mu.Unlock()

	return nil
}

func (r *RAFTRequests) Listen(ID int, reply *[]RAFTRequest) error {
	r.mu.Lock()
	*reply = r.Pending[ID]
	r.Pending[ID] = []RAFTRequest{}
	r.mu.Unlock()

	return nil
}

/*---------------*/

// Request struct represents a new message request to a client
type Request struct {
	ID    int
	Table *Membership
}

// Requests struct represents pending message requests
type Requests struct {
	mu      sync.Mutex // JY: Added mutex for synchronization
	Pending map[int]*Membership
}

// Returns a new instance of a Membership (pointer).
func NewRequests() *Requests {
	return &Requests{
		Pending: make(map[int]*Membership),
	}
}

// Adds a new message request to the pending list
func (req *Requests) Add(payload Request, reply *bool) error {
	// newMem = new value to be stored in req.Pending
	newMem := NewMembership()

	// Copy over the table from payload (without copying the mutex)
	for _, val := range payload.Table.Members {
		newMem.Add(val, &val)
	}

	// Now safely perform operations to req
	req.mu.Lock()
	// fmt.Pring("Requests Lock (Add)\n")

	// Check if an unread message has already been sent to the node
	oldMem, ok := req.Pending[payload.ID]
	// If there is, combine the tables
	if ok {
		newMem = CombineTables(oldMem, newMem)
	}

	// Update/Add the request to req
	req.Pending[payload.ID] = newMem

	// No longer performing operations on req
	// fmt.Pring("Requests Unlock (Add)\n")
	req.mu.Unlock()

	// Return Success
	return nil
}

// Listens to communication from neighboring nodes.
func (req *Requests) Listen(ID int, reply *Membership) error {
	// Default response is an new empty Membership
	*reply = *NewMembership()

	// Now performing operations on req
	req.mu.Lock()
	// fmt.Pring("Requests Lock (Listen)\n")

	// Ensure to unlock upon return
	// defer fmt.Pring("Requests Unlock (Listen)\n")
	defer req.mu.Unlock()

	// Check if ID has unread message(s)
	mem, ok := req.Pending[ID]
	// If not, return early
	if !ok {
		return nil
	}

	// If found, set reply members to found members
	for key, val := range mem.Members {
		reply.Members[key] = val
	}

	// Remove read message
	delete(req.Pending, ID)

	// Return success
	return nil
}

func CombineTables(table1 *Membership, table2 *Membership) *Membership {
	// Intialize return Membership
	ret := NewMembership()

	// Add all table1 members to the ret
	table1.mu.Lock()
	for key, val := range table1.Members {
		ret.Members[key] = val
	}
	table1.mu.Unlock()

	// Combine table2 into ret
	table2.mu.Lock()
	for key, val := range table2.Members {
		node, ok := ret.Members[key]

		// If ret doesn't have the member, add it
		if !ok {
			ret.Add(val, &val)
			continue
		}

		// If ret does have the member, update it only if table2's is newer
		if node.Time < val.Time {
			ret.Update(val, &val)
		}
	}
	table2.mu.Unlock()

	return ret
}

//mapreduce stuff

func (c *Coordinator) GiveOutTask(args *TaskRequest, reply *TaskReply) error {
	MRMutex.Lock()
	defer MRMutex.Unlock()

	// Assign Map Tasks
	allMapsDone := true
	for i, status := range MapTasks {
		if status == 0 { // Idle
			MapTasks[i] = 1 
			TaskTimestamps[fmt.Sprintf("map-%d", i)] = time.Now()

			// >>> PLACED HERE: Log the map assignment <<<
			c.Log = append(c.Log, LogEntry{
				Command: fmt.Sprintf("Assign-Map-%d-Worker-%d", i, args.WorkerID),
				Term:    args.Term, // The server can default this, or track current term
			})

			reply.TaskType = 1
			reply.TaskID = i
			reply.Filename = InputFiles[i]
			reply.NReduce = NReduce
			fmt.Printf("Leader State Machine: Logged assignment of Map %d\n", i)
			return nil
		}
		if status != 2 {
			allMapsDone = false
		}
	}

	if !allMapsDone {
		reply.TaskType = 0
		return nil
	}

	// Assign Reduce Tasks
	for i, status := range ReduceTasks {
		if status == 0 { // Idle
			ReduceTasks[i] = 1 
			TaskTimestamps[fmt.Sprintf("reduce-%d", i)] = time.Now()

			// >>> PLACED HERE: Log the reduce assignment <<<
			c.Log = append(c.Log, LogEntry{
				Command: fmt.Sprintf("Assign-Reduce-%d-Worker-%d", i, args.WorkerID),
				Term:    args.Term,
			})

			reply.TaskType = 2
			reply.TaskID = i
			reply.NMap = len(InputFiles)
			return nil
		}
	}

	reply.TaskType = 3
	return nil
}

func (c *Coordinator) CompleteTask(args *CompleteTaskArgs, reply *bool) error {
	MRMutex.Lock()
	defer MRMutex.Unlock()

	if args.TaskType == 1 {
		MapTasks[args.TaskID] = 2
		delete(TaskTimestamps, fmt.Sprintf("map-%d", args.TaskID))

		// >>> FIXED: Changed args.term to args.Term <<<
		c.Log = append(c.Log, LogEntry{
			Command: fmt.Sprintf("Complete-Map-%d", args.TaskID),
			Term:    args.Term, 
		})

	} else if args.TaskType == 2 {
		ReduceTasks[args.TaskID] = 2
		delete(TaskTimestamps, fmt.Sprintf("reduce-%d", args.TaskID))

		// >>> FIXED: Changed args.term to args.Term <<<
		c.Log = append(c.Log, LogEntry{
			Command: fmt.Sprintf("Complete-Reduce-%d", args.TaskID),
			Term:    args.Term, 
		})
	}

	*reply = true
	return nil
}

// GetLog allows the active RAFT Leader to fetch the server's state machine log
func (c *Coordinator) GetLog(args int, reply *[]LogEntry) error {
	MRMutex.Lock()
	defer MRMutex.Unlock()
	
	// Create a deep copy of the log to prevent concurrent slicing race conditions
	*reply = append([]LogEntry{}, c.Log...)
	return nil
}

