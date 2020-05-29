package mr

import "fmt"
import "log"
import "net/rpc"
import "encoding/json"
import "hash/fnv"
import "os"
import "io/ioutil"
import "time"
import "strings"

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	w := worker{
		mapf:    mapf,
		reducef: reducef,
	}

	for {
		task := requestTask()
		w.doTask(&task)
		time.Sleep(time.Millisecond * 500)
	}
}

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type worker struct {
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

func intermediateFilename(id int, inc int) string {
	return fmt.Sprintf("mr-%d-%d", id, inc)
}

func outputFilename(taskId int) string {
	return fmt.Sprintf("mr-out-%d", taskId)
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

func requestTask() Task {
	reply := Task{}

	if ok := call("Master.TaskRequest", &EmptyReq{}, &reply); !ok {
		fmt.Println("Master didn't respond. Closing worker.")
		os.Exit(0)
	}

	return reply
}

func (w *worker) doTask(task *Task) {
	switch task.Phase {
	case MapPhase:
		w.doMap(task)
	case ReducePhase:
		w.doReduce(task)
	default:
		panic(fmt.Sprintf("Unknown task phase: %v", task.Phase))
	}
}

func (w *worker) doMap(t *Task) {
	if len(t.Filename) == 0 {
		return
	}

	fmt.Printf("Working on: %v\n", t.Filename)

	content, err := ioutil.ReadFile(t.Filename)

	if err != nil {
		call("Master.ReportStatus", &StatusReportReq{Failed: true}, &EmptyRep{})
		return
	}

	kva := w.mapf(t.Filename, string(content))
	buckets := make([][]KeyValue, t.NReduce)

	for _, kv := range kva {
		idx := ihash(kv.Key) % t.NReduce
		buckets[idx] = append(buckets[idx], kv)
	}

	for i, bucket := range buckets {
		filename := intermediateFilename(t.Id, i)
		f, err := os.Create(filename)

		if err != nil {
			call("Master.ReportStatus", &StatusReportReq{Failed: true}, &EmptyRep{})
			return
		}

		enc := json.NewEncoder(f)

		// for each item in bucket, encode as JSON
		for _, kv := range bucket {
			enc.Encode(&kv)
		}

		if err := f.Close(); err != nil {
			call("Master.ReportStatus", &StatusReportReq{Failed: true}, &EmptyRep{})
			return
		}
	}

	status := StatusReportReq{
		Task: t,
		Done: true,
	}

	call("Master.ReportStatus", &status, &EmptyRep{})
}

func (w *worker) doReduce(t *Task) {
	maps := make(map[string][]string)

	fmt.Printf("Working on reduce: %v\n", t.Id)

	for i := 0; i < t.NMaps; i++ {
		filename := intermediateFilename(i, t.Id)
		file, err := os.Open(filename)

		if err != nil {
			fmt.Println("Failed to open %v", filename)
			call("Master.ReportStatus", &StatusReportReq{Failed: true}, &EmptyRep{})
			return
		}

		dec := json.NewDecoder(file)

		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}

			if _, ok := maps[kv.Key]; !ok {
				maps[kv.Key] = make([]string, 0, 100)
			}

			maps[kv.Key] = append(maps[kv.Key], kv.Value)
		}
	}

	res := make([]string, 0, 100)

	for k, v := range maps {
		res = append(res, fmt.Sprintf("%v %v\n", k, w.reducef(k, v)))
	}

	filename := outputFilename(t.Id)
	err := ioutil.WriteFile(filename, []byte(strings.Join(res, "")), 0600)

	if err != nil {
		status := StatusReportReq{
			Task:   t,
			Failed: true,
		}

		call("Master.ReportStatus", &status, &EmptyRep{})
		return
	}

	status := StatusReportReq{
		Task: t,
		Done: true,
	}

	call("Master.ReportStatus", &status, &EmptyRep{})
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
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
