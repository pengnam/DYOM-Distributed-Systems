package mr

//
// RPC definitions.
//

type GetJobRequest struct {}

type GetJobResponse struct {
	Job Job
}

type Job struct {
	JobType       JobType
	Filename      string
	Id            int
	NumberMaps	int
	NumberReduces int
}

type JobType int

const (
	Idle JobType = iota
	MapJob JobType = iota
	ReduceJob JobType = iota
	Done JobType = iota
)

type MarkJobCompletedRequest struct {
	Job Job
}

type MarkJobCompletedResponse struct {}
