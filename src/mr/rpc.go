package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type Args struct {
}

type Reply struct {
	Type        string
	Filename    string
	Nr          int
	Nm          int
	Mapindex    int
	Reduceindex int
	Stamp       string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

func GetUUID() string {
	out, err := exec.Command("uuidgen").Output()
	if err != nil {
		log.Fatal(err)
		return ""
	}
	return string(out)
}

var tmpdir = "./tmp"

func Intermediatename(mapindex, index int, stamp string) string {
	return fmt.Sprintf("%v/%v-mr-%v-%v", tmpdir, stamp, mapindex, index)
}

func Tmpname() string {
	return fmt.Sprintf("%v/%v", tmpdir, GetUUID())
}
