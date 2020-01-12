package mr

import (
	"container/list"
	"fmt"
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"


type Master struct {
	taskQueue *list.List
	ongoingTasks map[int]bool
	files []string
	phase JobType
	numReduces int
	numMaps int
	sync.Mutex
}
// TODO: Add sync primitives
// TODO: Add timeout
func (m *Master) GetJob(request *GetJobRequest, response *GetJobResponse) error {
	fmt.Println("A")
	m.CheckState()
	if m.hasNoUndoneTasks(){
		fmt.Println("B")
		response.Job = Job{
			JobType: Idle,
		}
		return nil
	}
	e := m.taskQueue.Front()
	m.taskQueue.Remove(e)
	taskId := e.Value.(int)

	response.Job = Job{
		Id:            taskId,
		JobType:       m.phase,
		NumberMaps: len(m.files),
		NumberReduces: m.numReduces,
	}
	if m.phase == MapJob {
		response.Job.Filename = m.files[taskId]
	}
	fmt.Println("C")
	return nil
}

func (m *Master) MarkJobCompleted (request *MarkJobCompletedRequest, response *MarkJobCompletedResponse) {
	delete(m.ongoingTasks, request.job.Id)
}

func (m *Master) CheckState() {
	switch (m.phase) {
	case MapJob:
		if m.hasNoTasks() {
			m.initialiseReducePhase()
		}
	case ReduceJob:
		if m.hasNoTasks() {
			m.phase = Idle
		}
	}
}

func (m *Master) initialiseReducePhase() {
	m.phase = ReduceJob
	for i := 0; i < m.numReduces; i++ {
		m.taskQueue.PushBack(i)
	}
}

func (m *Master) hasNoUndoneTasks() bool {
	return m.taskQueue.Len() == 0
}

func (m *Master) hasNoTasks() bool {
	return m.hasNoUndoneTasks() && len(m.ongoingTasks) == 0
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	os.Remove("mr-socket")
	l, e := net.Listen("unix", "mr-socket")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire Job has finished.
//
func (m *Master) Done() bool {
	return m.phase == ReduceJob && m.hasNoTasks()
}

//
// create a Master.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		taskQueue: list.New(),
		files: files,
		numReduces: nReduce,
		numMaps:len(files),
	}
	// Initialize
	m.phase = MapJob
	for i := 0; i < m.numMaps; i++ {
		m.taskQueue.PushBack(i)
	}
	m.server()
	return &m
}
