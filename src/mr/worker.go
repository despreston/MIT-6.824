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
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type worker struct {
	nReduce int
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

func requestTask() (Task, bool) {
	reply := Task{}

	if ok := call("Master.TaskRequest", EmptyReq{}, &reply); !ok {
		fmt.Println("Master didn't respond. Closing worker.")
		os.Exit(0)
	}

	if reply.Id == 0 {
		return reply, true
	}

	return reply, false
}

func readFile(filename string) string {
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

func (w *worker) register() {
	r := RegisterRep{}
	call("Master.RegisterWorker", EmptyReq{}, &r)
	w.nReduce = r.NReduce
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
	fmt.Printf("Working on: %v", t)

	content := readFile(t.Filename)
	// TODO: error handler

	kva := w.mapf(t.Filename, content)
	buckets := make([][]KeyValue, w.nReduce)

	for _, kv := range kva {
		idx := ihash(kv.Key) % w.nReduce
		buckets[idx] = append(buckets[idx], kv)
	}

	for i, bucket := range buckets {
		filename := intermediateFilename(t.Id, i)
		f, err := os.Create(filename)
		enc := json.NewEncoder(f)

		if err != nil {
			// TODO: error handler
		}

		// for each item in bucket, encode as JSON
		for _, kv := range bucket {
			enc.Encode(&kv)
			// TODO: error handler
		}

		if err := f.Close(); err != nil {
			// TODO: error handler
		}
	}

	status := StatusReportReq{
		Task: t,
		Done: true,
	}

	call("Master.ReportStatus", status, EmptyRep{})
}

func (w *worker) doReduce(t *Task) {
	maps := make(map[string][]string)

	for i := 0; i < t.NMaps; i++ {
		filename := intermediateFilename(i, t.Id)
		fmt.Printf("File: %v", filename)
		file, err := os.Open(filename)

		if err != nil {
			// TODO: handle error
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
		res = append(res, fmt.Sprintf("%v %v\n"), k, w.reducef(k, v))
	}

	filename := outputFilename(t.Id)
	err := ioutil.WriteFile(filename, []byte(strings.Join(res, "")), 0600)

	if err != nil {
		status := StatusReportReq{
			Task:   t,
			Failed: true,
		}

		call("Master.ReportStatus", status, EmptyRep{})
		return
	}

	status := StatusReportReq{
		Task: t,
		Done: true,
	}

	call("Master.ReportStatus", status, EmptyRep{})
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	w := worker{
		mapf:    mapf,
		reducef: reducef,
	}

	w.register()

	for {
		task, empty := requestTask()

		if !empty {
			w.doTask(&task)
		}

		time.Sleep(time.Millisecond * 500)
	}
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
