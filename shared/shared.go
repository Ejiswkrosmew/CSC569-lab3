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

// Intermediate data structure
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//-- map reduce stuff

type IntFile struct {
	WorkerID  int
	Partition int
}

type LogEntry struct {
	Type     int // 0 = Map, 1 = Reduce
	Status   int // 0 = Assign, 1 = Complete
	ID       int // MapID/Key
	WorkerID int
	Term     int //election term
}

func (e LogEntry) Copy() *LogEntry {
	return &LogEntry{
		Type:     e.Type,
		Status:   e.Status,
		ID:       e.ID,
		WorkerID: e.WorkerID,
		Term:     e.Term,
	}
}

var InputFiles = []string{"pg-being_ernest.txt", "pg-metamorphosis.txt"}
var NReduce = 3

// Task tracking: 0 = Idle, 1 = In Progress, 2 = Completed
var MapTasks []int
var ReduceTasks []int
var ReduceFiles [][]IntFile
var TaskTimestamps map[string]time.Time

var MRMutex sync.Mutex

type Coordinator struct {
	Log []LogEntry //centralized log
} //for registering to server

type TaskRequest struct {
	WorkerID int
}

type TaskReply struct {
	TaskType    int // 0 = Wait, 1 = Map, 2 = Reduce, 3 = All Done
	TaskID      int
	Filename    string
	NReduce     int
	NMap        int
	ReduceFiles []IntFile
}

type CompleteTaskArgs struct {
	TaskType int // 1 = Map, 2 = Reduce
	TaskID   int
	NewFiles []IntFile
}

type RAFTNode struct {
	State       int // 0: follower, 1: candidate, 2: leader
	Term        int
	Vote        int
	Votes       int
	Leader      int
	Log         []LogEntry //log
	CommitIndex int        //index of highest entry committed
	LastApplied int        //index of highest entry applied to state machine
	//for the leader
	NextIndex  map[int]int //next entry to send to each peer
	MatchIndex map[int]int //highest mirrored per peer
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
	// If you are running exactly nodes 1, 2, 3, and 4:
	// We want a ring: 1 -> 2 -> 3 -> 4 -> 1

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
	To           int
	From         int
	Term         int
	Type         int              // 0: request, 1: vote, 2: leader-ping, 3: follower reply, 4: request job, 5: give job
	PrevLogIndex int              //index of entry just before new ones
	PrevLogTerm  int              //term of above
	Entries      []LogEntry       //logs to store
	LeaderCommit int              //leaders commit index
	Success      bool             //follower replication status reply
	Task         TaskReply        // 5: Task for the worker to do
	Completed    CompleteTaskArgs // 6
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

	fmt.Printf("Leader: Received TaskRequest from Worker %d\n", args.WorkerID)

	//assign tasks
	allMapsDone := true
	for i, status := range MapTasks {
		if status == 0 { // Idle
			//instead of just changing state we have
			//to do the log stuff
			c.Log = append(c.Log, LogEntry{
				Type:     0,
				Status:   0,
				ID:       i,
				WorkerID: args.WorkerID,
				Term:     0, //will be updated later by leader
			})

			reply.TaskType = 1
			reply.TaskID = i
			reply.Filename = InputFiles[i]
			reply.NReduce = NReduce
			fmt.Printf("Leader State Machine: Logged staging of Map Task %d\n", i)
			return nil
		}
		if status != 2 {
			fmt.Printf("Leader: Telling Worker %d to WAIT. Map tasks are still in progress.\n", args.WorkerID)
			allMapsDone = false
		}
	}

	// workers must wait
	if !allMapsDone {
		reply.TaskType = 0
		return nil
	}

	// 3. Check Reduce Phase
	for i, status := range ReduceTasks {
		if status == 0 { // Idle
			c.Log = append(c.Log, LogEntry{
				Type:     1,
				Status:   0,
				ID:       i,
				WorkerID: args.WorkerID,
				Term:     0,
			})

			reply.TaskType = 2
			reply.TaskID = i
			reply.NMap = len(InputFiles)
			reply.ReduceFiles = ReduceFiles[i]
			fmt.Printf("Leader State Machine: Logged staging of Reduce Task %d\n", i)
			return nil
		}
	}

	// 4. Job Complete
	reply.TaskType = 3
	fmt.Printf("Leader: Telling Worker %d all tasks are COMPLETED.\n", args.WorkerID)
	return nil
}

func (c *Coordinator) CompleteTask(args *CompleteTaskArgs, reply *bool) error {
	MRMutex.Lock()
	defer MRMutex.Unlock()

	//rmark map done
	if args.TaskType == 1 {
		for b, newFile := range args.NewFiles {
			fmt.Printf("Adding to task %d: %v\n", b, newFile)
			files := ReduceFiles[b]

			exists := false
			for _, f := range files {
				if f == newFile {
					exists = true
					break
				}
			}

			if !exists {
				ReduceFiles[b] = append(files, newFile)
			}
		}

		c.Log = append(c.Log, LogEntry{
			Type:   0,
			Status: 1,
			ID:     args.TaskID,
			Term:   0,
		})
		fmt.Printf("Leader State Machine: Logged completion intent for Map Task %d\n", args.TaskID)
		//mark reduce done
	} else if args.TaskType == 2 {
		c.Log = append(c.Log, LogEntry{
			Type:   1,
			Status: 1,
			ID:     args.TaskID,
			Term:   0,
		})
		fmt.Printf("Leader State Machine: Logged completion intent for Reduce Task %d\n", args.TaskID)
	}

	*reply = true
	return nil
}

// GetLog allows the client-side RAFT Leader node to pull down staged logs from the server
func (c *Coordinator) GetLog(leaderCommit int, reply *[]LogEntry) error {
	MRMutex.Lock()
	defer MRMutex.Unlock()

	// Create a deep slice copy to prevent concurrent thread manipulation race conditions
	*reply = append([]LogEntry{}, c.Log...)

	//update state based on logs
	if leaderCommit >= 0 && leaderCommit < len(c.Log) {
		for i := 0; i <= leaderCommit; i++ {
			entry := c.Log[i]
			taskId := entry.ID
			// workerID := entry.WorkerID

			if entry.Type == 0 {
				// Map type
				if entry.Status == 0 && MapTasks[taskId] == 0 {
					// Assign (Only if idle)
					MapTasks[taskId] = 1
				} else if entry.Status == 1 {
					//Completed
					MapTasks[taskId] = 2
				}
			} else if entry.Type == 1 {
				// Reduce type
				if entry.Status == 0 && ReduceTasks[taskId] == 0 {
					// Assign (Only if idle)
					ReduceTasks[taskId] = 1
				} else if entry.Status == 1 {
					//Completed
					ReduceTasks[taskId] = 2
				}
			}
		}
	}

	return nil
}
