package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"encoding/gob"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"


// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		job := GetJobFromServer()
		switch job.jobType {
		case MapJob:
			handleMapJob(mapf, job)
		case ReduceJob:
			fmt.Println("Reduce job")
		default:
			log.Fatalf("Neither map or reduce")
		}
	}
}

func openFile(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return string(content)
}

func handleMapJob (mapf func (string, string) []KeyValue, job Job) {
	fmt.Println("Map job")

	kva := mapf(job.filename, openFile(job.filename))

	result := partition(kva)
	for i, val := range result {
		saveKva(val, "test-"+string(i) + "-" + string(job.id))
	}
}

func partition(kva []KeyValue) [][]KeyValue{
	var result [][]KeyValue
	result = make([][]KeyValue, 10)
	for _, kv := range kva {
		hash := ihash(kv.Key)
		result[hash] = append(result[hash], kv)
	}
	return result
}

func saveKva(kva []KeyValue, outputFilename string) {
	f, err := os.Create(outputFilename)
	if err != nil {
		log.Fatalf("cannot create %v", outputFilename)
	}
	encoder := gob.NewEncoder(f)
	err = encoder.Encode(kva)
	if err != nil {
		panic(err)
	}
}

func openKva(filename string) []KeyValue{
	var kva []KeyValue
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	decoder := gob.NewDecoder(file)
	decoder.Decode(&kva)
	return kva
}

func handleReduceJob(reducef func(string, []string) string, job Job) {
	oname := "mr-out-" + string(job.id)
	ofile, _ := os.Create(oname)
	intermediate := []KeyValue{}
	for i := 0; i < 10; i++ {
		intermediate = append(intermediate, openKva("test-" + string(job.id) + "-" + string(i))...)
	}
	sort.Sort(ByKey(intermediate))
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
}

func GetJobFromServer() Job {

	// declare an argument structure.
	args := GetJobRequest{}

	// declare a reply structure.
	reply := GetJobResponse{}

	// send the RPC request, wait for the reply.
	call("Master.GetJob", &args, &reply)

	return reply.job
}

func DeclareFinish() {

}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("unix", "mr-socket")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
