package mr

//
// RPC definitions.
//

type GetJobRequest struct {}

type GetJobResponse struct {
	job Job
}

type Job struct {
	jobType JobType
	filename string
	id int
}

type JobType int

const (
	MapJob JobType = iota
	ReduceJob JobType = iota
)

type MarkJobCompletedRequest struct {}

type MarkJobCompletedResponse struct {}
