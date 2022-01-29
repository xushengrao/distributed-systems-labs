package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type MasterTaskState int

const (
	Idle MasterTaskState = iota
	InProgress
	Completed
)

type State int

const (
	Map State = iota
	Reduce
	Exit
	Wait
)

type Task struct {
	Input         string
	TaskState     State
	NReducer      int
	TaskNumber    int
	Intermediates []string
	Output        string
}

type MasterTask struct {
	TaskState     MasterTaskState
	StartTime     time.Time
	TaskReference *Task
}

type Coordinator struct {
	TaskQueue     chan *Task
	TaskMeta      map[int]*MasterTask
	MasterPhase   State
	NReduce       int
	InputFiles    []string
	Intermediates [][]string
}

var mu sync.Mutex

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	mu.Lock()
	defer mu.Unlock()
	ret := c.MasterPhase == Exit
	return ret
}

func (c *Coordinator) creatMapTask() {
	for i, filename := range c.InputFiles {
		task := Task{
			Input:      filename,
			TaskState:  Map,
			NReducer:   c.NReduce,
			TaskNumber: i,
		}
		//fmt.Println(task.TaskState)
		c.TaskQueue <- &task
		c.TaskMeta[i] = &MasterTask{
			TaskState:     Idle,
			TaskReference: &task,
		}
	}
}

func (c *Coordinator) creatReduceTask() {
	for i, files := range c.Intermediates {
		task := Task{
			TaskState:     Reduce,
			NReducer:      c.NReduce,
			TaskNumber:    i,
			Intermediates: files,
		}
		c.TaskQueue <- &task
		c.TaskMeta[i] = &MasterTask{
			TaskState:     Idle,
			TaskReference: &task,
		}
	}
}

func (c *Coordinator) AssignTask(args *ExampleArgs, reply *Task) error {
	//fmt.Println("assigntask")
	mu.Lock()
	//fmt.Println("lock")
	defer mu.Unlock()
	//fmt.Println(len(c.TaskQueue))
	if len(c.TaskQueue) > 0 {
		// ----------reply = <-m.TaskQueue ?
		//fmt.Println(len(c.TaskQueue))
		*reply = *<-c.TaskQueue
		c.TaskMeta[reply.TaskNumber].TaskState = InProgress
		c.TaskMeta[reply.TaskNumber].StartTime = time.Now()
	} else if c.MasterPhase == Exit {
		*reply = Task{TaskState: Exit}
	} else {
		*reply = Task{TaskState: Wait}
	}
	return nil
}

func (c *Coordinator) TaskCompleted(task *Task, reply *ExampleReply) error {
	mu.Lock()
	defer mu.Unlock()
	if task.TaskState != c.MasterPhase || c.TaskMeta[task.TaskNumber].TaskState == Completed {
		return nil
	}
	c.TaskMeta[task.TaskNumber].TaskState = Completed
	go c.processTaskResult(task)
	return nil
}

func (c *Coordinator) processTaskResult(task *Task) {
	mu.Lock()
	defer mu.Unlock()
	switch task.TaskState {
	case Map:
		//收集intermediate信息
		for taskId, filePath := range task.Intermediates {
			c.Intermediates[taskId] = append(c.Intermediates[taskId], filePath)
		}
		if c.allTaskDone() {
			c.creatReduceTask()
			c.MasterPhase = Reduce
		}
	case Reduce:
		if c.allTaskDone() {
			c.MasterPhase = Exit
		}
	}

}

func (c *Coordinator) allTaskDone() bool {
	for _, task := range c.TaskMeta {
		if task.TaskState != Completed {
			return false
		}
	}
	return true
}

func (c *Coordinator) catchTimeOut() {
	for {
		time.Sleep(5 * time.Second)
		mu.Lock()
		if c.MasterPhase == Exit {
			mu.Unlock()
			return
		}
		for _, masterTask := range c.TaskMeta {
			if masterTask.TaskState == InProgress && time.Now().Sub(masterTask.StartTime) > 10*time.Second {
				c.TaskQueue <- masterTask.TaskReference
				masterTask.TaskState = Idle
			}
		}
		mu.Unlock()
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		TaskQueue:     make(chan *Task, max(nReduce, len(files))),
		TaskMeta:      make(map[int]*MasterTask),
		MasterPhase:   Map,
		NReduce:       nReduce,
		InputFiles:    files,
		Intermediates: make([][]string, nReduce),
	}

	c.creatMapTask()
	//fmt.Println("1")
	c.server()
	//fmt.Println("2")
	go c.catchTimeOut()
	return &c
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}
