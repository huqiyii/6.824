package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Master struct {
	// Your definitions here.
	inputname []string
	nr        int
	timetask  map[string]time.Time
	mutex     sync.Mutex
	stat      bool
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) judgemap(index int) bool {
	for i := 0; i < m.nr; i++ {
		name := fmt.Sprintf("mr-%v-%v", index, i)
		if _, err := os.Stat(name); err != nil && os.IsNotExist(err) {
			return false
		}
	}
	return true
}

func (m *Master) MapTask() (string, int) {
	for index, name := range m.inputname {
		if timebegin, ok := m.timetask[fmt.Sprintf("%v-%v", index, "map")]; !ok || (time.Now().After(timebegin) && !m.judgemap(index)) {
			return name, index
		}
	}
	return "", -1
}

func (m *Master) ReduceTask() int {
	for i := 0; i < m.nr; i++ {
		timebegin, ok := m.timetask[fmt.Sprintf("%v-%v", i, "reduce")]
		if !ok {
			return i
		}
		if _, err := os.Stat(fmt.Sprintf("mr-out-%v", i)); time.Now().After(timebegin) && err != nil && os.IsNotExist(err) {
			return i
		}
	}
	return -1
}

func (m *Master) Apply(args *Args, reply *Reply) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	key := ""
	if mapname, index := m.MapTask(); mapname != "" && index != -1 {
		reply.Filename = mapname
		reply.Type = "map"
		reply.Nr = m.nr
		reply.Mapindex = index
		key = fmt.Sprintf("%v-%v", reply.Mapindex, reply.Type)
	} else if reduceindex := m.ReduceTask(); reduceindex >= 0 {
		reply.Nm = len(m.inputname)
		reply.Nr = m.nr
		reply.Reduceindex = reduceindex
		reply.Type = "reduce"
		key = fmt.Sprintf("%v-%v", reply.Reduceindex, reply.Type)
	} else {
		reply.Type = "exit"
		m.stat = true
	}
	m.timetask[key] = time.Now().Add(time.Second * 10)
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
	return m.stat
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.nr = nReduce
	m.inputname = files
	m.mutex = sync.Mutex{}
	m.timetask = make(map[string]time.Time)
	// Your code here.

	m.server()
	return &m
}
