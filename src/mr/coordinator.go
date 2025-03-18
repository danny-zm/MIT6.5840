package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// Phase coordinator phase
type Phase int

const (
	MapPhase Phase = iota
	ReducePhase
	WaitPhase
	ExitPhase
)

type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

type Task struct {
	StartTime         time.Time
	IntermediateFiles []string
	InputFilePath     string
	OutputFile        string
	ID                int
	NReducer          int
	Phase             Phase
	Status            TaskStatus
}

type Coordinator struct {
	IntermediateFiles    [][]string
	InputFilePaths       []string
	MapTasks             map[int]*Task
	ReduceTasks          map[int]*Task
	idleMapTasks         chan *Task
	idleReduceTasks      chan *Task
	Phase                Phase
	NReduce              int
	completedMapTasks    int
	completedReduceTasks int
	mu                   sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(args *ExampleArgs, reply *Task) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch c.Phase {
	case MapPhase, ReducePhase:
		taskChan := c.idleMapTasks
		if c.Phase == ReducePhase {
			taskChan = c.idleReduceTasks
		}
		select {
		case task := <-taskChan:
			task.Status = InProgress
			task.StartTime = time.Now()
			*reply = *task
		default:
			*reply = Task{
				Phase: WaitPhase,
			}
		}
	case ExitPhase:
		*reply = Task{
			Phase: ExitPhase,
		}
	}
	return nil
}

func (c *Coordinator) TaskCompleted(task *Task, reply *ExampleReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if task.Phase != c.Phase ||
		task.Phase == MapPhase && c.MapTasks[task.ID].Status == Completed ||
		task.Phase == ReducePhase && c.ReduceTasks[task.ID].Status == Completed {
		return nil
	}
	if task.Phase == MapPhase {
		c.MapTasks[task.ID].Status = Completed
		c.completedMapTasks++
	} else {
		c.ReduceTasks[task.ID].Status = Completed
		c.completedReduceTasks++
	}
	go c.processTaskResult(task)
	return nil
}

func (c *Coordinator) processTaskResult(task *Task) {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch task.Phase {
	case MapPhase:
		for reduceTaskID, filePath := range task.IntermediateFiles {
			c.IntermediateFiles[reduceTaskID] = append(c.IntermediateFiles[reduceTaskID], filePath)
		}
		if c.allMapTasksDone() {
			c.createReduceTask()
			c.Phase = ReducePhase
		}
	case ReducePhase:
		if c.allReduceTasksDone() {
			c.Phase = ExitPhase
		}
	}
}

func (c *Coordinator) allMapTasksDone() bool {
	return c.completedMapTasks == len(c.MapTasks)
}

func (c *Coordinator) allReduceTasksDone() bool {
	return c.completedReduceTasks == len(c.ReduceTasks)
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.Phase == ExitPhase
}

// CreatemapTask creates map tasks
func (c *Coordinator) CreateMapTask() {
	for idx, filename := range c.InputFilePaths {
		task := Task{
			ID:                idx,
			StartTime:         time.Now(),
			Status:            Idle,
			InputFilePath:     filename,
			NReducer:          c.NReduce,
			Phase:             MapPhase,
			IntermediateFiles: []string{},
		}
		c.idleMapTasks <- &task
		c.MapTasks[idx] = &task
	}
}

func (c *Coordinator) createReduceTask() {
	for idx, files := range c.IntermediateFiles {
		task := Task{
			ID:                idx,
			StartTime:         time.Now(),
			Status:            Idle,
			NReducer:          c.NReduce,
			Phase:             ReducePhase,
			IntermediateFiles: files,
		}
		c.idleReduceTasks <- &task
		c.ReduceTasks[idx] = &task
	}
}

func (c *Coordinator) catchTimeout() {
	for {
		time.Sleep(5 * time.Second)

		c.mu.Lock()
		switch c.Phase {
		case ExitPhase:
			c.mu.Unlock()
			return
		case MapPhase:
			for _, task := range c.MapTasks {
				if task.Status == InProgress && time.Since(task.StartTime) > 10*time.Second {
					task.Status = Idle
					c.idleMapTasks <- task
				}
			}
		case ReducePhase:
			for _, task := range c.ReduceTasks {
				if task.Status == InProgress && time.Since(task.StartTime) > 10*time.Second {
					task.Status = Idle
					c.idleReduceTasks <- task
				}
			}
		}
		c.mu.Unlock()
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// splits the input files
	c := Coordinator{
		InputFilePaths:    files,
		Phase:             MapPhase,
		NReduce:           nReduce,
		MapTasks:          map[int]*Task{},
		ReduceTasks:       map[int]*Task{},
		IntermediateFiles: make([][]string, nReduce),
		idleMapTasks:      make(chan *Task, max(nReduce, len(files))),
		idleReduceTasks:   make(chan *Task, max(nReduce, len(files))),
		mu:                sync.Mutex{},
	}
	// create map tasks
	c.CreateMapTask()
	c.server()
	// check timeout task
	go c.catchTimeout()
	return &c
}
