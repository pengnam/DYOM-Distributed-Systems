package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

type WorkerStatus int

const (
	Idle WorkerStatus = iota
	Working WorkerStatus = iota
)


type Master struct {
	workers map[string]WorkerStatus
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) Poll(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	os.Remove("mr-socket")
	l, e := net.Listen("unix", "mr-socket")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// create a Master.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.


	return &m
}
