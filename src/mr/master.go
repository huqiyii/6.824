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
	stamp     string
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
		name := Intermediatename(index, i, m.stamp)
		if _, err := os.Stat(name); err != nil && os.IsNotExist(err) {
			return false
		}
	}
	return true
}

func (m *Master) mapTask(reply *Reply) {
	done := true
	for index, name := range m.inputname {
		if timebegin, ok := m.timetask[fmt.Sprintf("%v-0-%v", index, "map")]; !ok || (time.Now().After(timebegin) && !m.judgemap(index)) {
			reply.Filename = name
			reply.Type = "map"
			reply.Nr = m.nr
			reply.Mapindex = index
			return
		}
		if !m.judgemap(index) {
			done = false
		}
	}
	if !done {
		reply.Type = "wait"
	}
}

func (m *Master) reduceTask(reply *Reply) {
	done := true
	for i := 0; i < m.nr; i++ {
		timebegin, ok := m.timetask[fmt.Sprintf("0-%v-%v", i, "reduce")]
		_, err := os.Stat(fmt.Sprintf("mr-out-%v", i))
		if !ok || (time.Now().After(timebegin) && err != nil && os.IsNotExist(err)) {
			reply.Nm = len(m.inputname)
			reply.Nr = m.nr
			reply.Reduceindex = i
			reply.Type = "reduce"
			return
		}
		if err != nil && os.IsNotExist(err) {
			done = false
		}
	}
	if !done {
		reply.Type = "wait"
	}
}

func (m *Master) Apply(args *Args, reply *Reply) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.mapTask(reply); reply.Type == "" {
		if m.reduceTask(reply); reply.Type == "" {
			reply.Type = "exit"
			m.stat = true
		}
	}
	key := fmt.Sprintf("%v-%v-%v", reply.Mapindex, reply.Reduceindex, reply.Type)
	m.timetask[key] = time.Now().Add(time.Second * 10)
	reply.Stamp = m.stamp
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
	m.stamp = GetUUID()
	// Your code here.

	m.server()
	return &m
}
