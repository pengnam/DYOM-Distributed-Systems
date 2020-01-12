package mr

import (
	"container/list"
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"


type Master struct {
	taskQueue *list.List
	ongoingTasks map[int]chan int
	files []string
	phase JobType
	numReduces int
	numMaps int
	sync.Mutex
}
func (m *Master) GetJob(request *GetJobRequest, response *GetJobResponse) error {
	m.Lock()
	m.CheckState()
	if m.hasNoUndoneTasks(){
		// There should not be undone
		fmt.Println(m.ongoingTasks)
		fmt.Println(m.taskQueue)
		if m.phase == Done {
			response.Job = Job{
				JobType: Done,
			}
		} else {
			response.Job = Job{
				JobType: Idle,
			}
		}
		m.Unlock()
		return nil
	}
	e := m.taskQueue.Front()
	m.taskQueue.Remove(e)
	taskId := e.Value.(int)
	m.ongoingTasks[taskId] = m.createTimeout(taskId)

	response.Job = Job{
		Id:            taskId,
		JobType:       m.phase,
		NumberMaps: len(m.files),
		NumberReduces: m.numReduces,
	}
	if m.phase == MapJob {
		response.Job.Filename = m.files[taskId]
	}
	m.Unlock()
	return nil
}

func (m *Master) createTimeout(jobId int) chan int{
	c := make(chan int)
	t := time.After(5 * time.Second)
	go func() {
		select {
			case <- t:
				m.Lock()
				fmt.Println("TIMEOUT!!!!!!!")
				fmt.Println(m.ongoingTasks)
				fmt.Println(m.taskQueue)
				delete(m.ongoingTasks, jobId)
				m.taskQueue.PushBack(jobId)

				m.Unlock()
			case <- c:
				m.Lock()
				delete(m.ongoingTasks, jobId)
				m.Unlock()
		}
	}()
	return c
}

func (m *Master) MarkJobCompleted (request *MarkJobCompletedRequest, response *MarkJobCompletedResponse) error {
	m.Lock()
	if _, ok := m.ongoingTasks[request.Job.Id]; ok {
		m.ongoingTasks[request.Job.Id] <- 1
	}
	m.Unlock()
	return nil
}

// Not thread safe
func (m *Master) CheckState() {
	switch (m.phase) {
	case MapJob:
		if m.hasNoTasks() {
			m.initialiseReducePhase()
		}
	case ReduceJob:
		if m.hasNoTasks() {
			m.phase = Done
		}
	}
}

// Not thread safe
func (m *Master) initialiseReducePhase() {
	m.phase = ReduceJob
	for i := 0; i < m.numReduces; i++ {
		m.taskQueue.PushBack(i)
	}
}
// Not thread safe
func (m *Master) hasNoUndoneTasks() bool {
	return m.taskQueue.Len() == 0
}

// Not thread safe
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
	m.Lock()
	m.CheckState()
	status := m.phase == Done
	m.Unlock()
	return status
}

//
// create a Master.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		taskQueue: list.New(),
		ongoingTasks: make(map[int]chan int),
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
