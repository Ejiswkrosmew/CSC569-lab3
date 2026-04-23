package shared

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

const (
	MAX_NODES = 8
)

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
	neighbor1 := RandInt()
	for neighbor1 == id {
		neighbor1 = RandInt()
	}
	neighbor2 := RandInt()
	for neighbor1 == neighbor2 || neighbor2 == id {
		neighbor2 = RandInt()
	}
	return [2]int{neighbor1, neighbor2}
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
