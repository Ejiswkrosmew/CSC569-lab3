package main

import (
	"io"
	"lab3/shared"
	"net/http"
	"net/rpc"
)

func main() {
	// create a Membership list
	nodes := shared.NewMembership()
	requests := shared.NewRequests()
	RAFTrequests := shared.NewRAFTRequests()

	//init mr stuff
	// shared.MRMutex.Lock()
	// //TODO: -1 is idle, 0 is done, all other ints are node id assigned
	// shared.MapTasks = make([]int, len(shared.InputFiles))
	// shared.ReduceTasks = make([]int, shared.NReduce)
	// shared.TaskTimestamps = make(map[string]time.Time)
	// shared.MRMutex.Unlock()

	// coordinator := new(shared.Coordinator)
	// rpc.Register(coordinator) //mapreduce register

	// register nodes with `rpc.DefaultServer`
	rpc.Register(nodes)
	rpc.Register(requests)
	rpc.Register(RAFTrequests)

	// register an HTTP handler for RPC communication
	rpc.HandleHTTP()

	// sample test endpoint
	http.HandleFunc("/", func(res http.ResponseWriter, req *http.Request) {
		io.WriteString(res, "RPC SERVER LIVE!")
	})

	// listen and serve default HTTP server
	http.ListenAndServe("localhost:9005", nil)
}
