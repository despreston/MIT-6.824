package mr

import "log"
import "fmt"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"

type TaskPhase int
type taskStatus int

const (
	MapPhase    TaskPhase     = 0
	ReducePhase TaskPhase     = 1
	taskTimeout time.Duration = time.Second * 10
)

const (
	taskRunning  taskStatus = 0
	taskQueued   taskStatus = 1
	taskFinished taskStatus = 2
)

type Task struct {
	Filename string
	Id       int
	NMaps    int
	NReduce  int
	Phase    TaskPhase
}

type TaskMeta struct {
	taskId int
	status taskStatus
}

type Master struct {
	nReduce   int
	taskCount int
	NMaps     int
	tasks     []Task
	tasksMeta []TaskMeta
	mux       sync.Mutex
	files     []string
	phase     TaskPhase
}

func (m *Master) getTaskId() int {
	count := m.taskCount
	m.taskCount += 1
	return count
}

func initTimeout(m *Master, task *Task) {
	time.Sleep(taskTimeout)

	if m.tasksMeta[task.Id].status == taskFinished {
		return
	}

	m.mux.Lock()
	defer m.mux.Unlock()

	// Worker's taken longer than 10 seconds. Change status so it'll get picked by
	// another worker.
	m.tasksMeta[task.Id].status = taskQueued
	fmt.Printf("Worker took too long. Putting %v back\n", task)
}

func (m *Master) TaskRequest(_ *EmptyReq, reply *Task) error {
	m.mux.Lock()
	defer m.mux.Unlock()

	task := Task{}

	for _, t := range m.tasksMeta {
		if t.status == taskQueued {
			task = m.tasks[t.taskId]
		}
	}

	if task.Id == 0 {
		return nil
	}

	m.tasksMeta[task.Id].status = taskRunning

	reply.Id = task.Id
	reply.Filename = task.Filename
	reply.NMaps = task.NMaps
	reply.NReduce = task.NReduce
	reply.Phase = task.Phase

	go initTimeout(m, reply)

	fmt.Printf("Assigning task to worker: %v\n", reply.Id)

	return nil
}

func (m *Master) ReportStatus(req *StatusReportReq, _ *EmptyRep) error {
	m.mux.Lock()
	defer m.mux.Unlock()

	if req.Done {
		m.tasksMeta[req.Task.Id].status = taskFinished
		m.NMaps += 1
	}

	if req.Failed {
		m.tasksMeta[req.Task.Id].status = taskQueued
	}

	allFinished := true
	for _, v := range m.tasksMeta {
		if v.taskId != 0 && v.status != taskFinished {
			allFinished = false
			break
		}
	}

	if allFinished {
		if m.phase == MapPhase {
			m.phase = ReducePhase
			m.tasks = make([]Task, m.nReduce)
			m.tasksMeta = make([]TaskMeta, m.nReduce)
			m.enqueueReduceTasks()
		} else {
			// Finished !
			fmt.Println("Finished")
			os.Exit(0)
		}
	}

	return nil
}

func (m *Master) RegisterWorker(_ *EmptyReq, reply *RegisterRep) error {
	m.mux.Lock()
	defer m.mux.Unlock()

	reply.NReduce = m.nReduce

	fmt.Printf("Registering worker\n")

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
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
	return len(m.files) == 0
}

func (m *Master) enqueueReduceTasks() {
	m.taskCount = 0
	m.tasksMeta = make([]TaskMeta, m.nReduce)
	m.tasks = make([]Task, m.nReduce)

	for i := 0; i < m.nReduce; i++ {
		t := Task{
			Id:      m.getTaskId(),
			NMaps:   m.NMaps,
			NReduce: m.nReduce,
			Phase:   ReducePhase,
		}

		fmt.Printf("Enqueueing reduce job: %v\n", t)

		m.tasks[t.Id] = t

		m.tasksMeta[t.Id] = TaskMeta{
			taskId: t.Id,
			status: taskQueued,
		}
	}
}

func (m *Master) enqueueMapTasks() {
	m.mux.Lock()
	defer m.mux.Unlock()

	m.tasksMeta = make([]TaskMeta, len(m.files))
	m.tasks = make([]Task, len(m.files))

	for _, v := range m.files {
		fmt.Printf("Enqueueing map job: %v\n", v)

		t := Task{
			Id:       m.getTaskId(),
			NMaps:    m.NMaps,
			NReduce:  m.nReduce,
			Filename: v,
			Phase:    MapPhase,
		}

		m.tasks[t.Id] = t

		m.tasksMeta[t.Id] = TaskMeta{
			taskId: t.Id,
			status: taskQueued,
		}
	}
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		files:   files,
		nReduce: nReduce,
		phase:   MapPhase,
	}

	m.enqueueMapTasks()
	m.server()
	return &m
}
