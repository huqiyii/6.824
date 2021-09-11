package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	args := new(Args)
	for {
		reply := new(Reply)
		call("Master.Apply", args, reply)
		switch reply.Type {
		case "map":
			MapTask(mapf, reply)
		case "reduce":
			ReduceTask(reducef, reply)
		case "exit":
			return
		}
	}

}

func MapTask(mapf func(string, string) []KeyValue, reply *Reply) error {
	content, err := Readfile(reply.Filename)
	if err != nil {
		return err
	}
	mapout := mapf(reply.Filename, content)
	buckets := make([][]KeyValue, reply.Nr)
	for _, kv := range mapout {
		reduceindex := ihash(kv.Key) % reply.Nr
		if buckets[reduceindex] == nil || len(buckets[reduceindex]) == 0 {
			buckets[reduceindex] = make([]KeyValue, 0)
		}
		buckets[reduceindex] = append(buckets[reduceindex], kv)
	}
	for index, intermediate := range buckets {
		name := fmt.Sprintf("mr-%v-%v", reply.Mapindex, index)
		if file, err := os.Create(name + "temp"); err == nil {
			enc := json.NewEncoder(file)
			for _, kv := range intermediate {
				enc.Encode(&kv)
			}
		} else {
			log.Fatalf("写入中间文件出错, filename: %v, err:%v", name, err)
			return fmt.Errorf("写入中间文件出错, filename: %v", name)
		}
		if err := os.Rename(name+"temp", name); err != nil {
			log.Fatalf("修改文件名字出错, filename: %v, err:%v", name, err)
			return fmt.Errorf("修改文件名字出错, filename: %v", name)
		}
	}
	return nil
}

func ReduceTask(reducef func(string, []string) string, reply *Reply) error {
	intermediate := make([]KeyValue, 0)
	for i := 0; i < reply.Nm; i++ {
		filename := fmt.Sprintf("mr-%v-%v", i, reply.Reduceindex)
		kva, err := Readjson(filename)
		if err != nil {
			return err
		}
		intermediate = append(intermediate, kva...)
	}
	sort.Sort(ByKey(intermediate))

	outputname := fmt.Sprintf("mr-out-%v", reply.Reduceindex)
	ofile, err := os.Create(outputname + "temp")
	if err != nil {
		return err
	}

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
	if err := os.Rename(outputname+"temp", outputname); err != nil {
		return err
	}
	return nil
}

func Readjson(filename string) ([]KeyValue, error) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return nil, err
	}
	intermediate := make([]KeyValue, 0)
	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		intermediate = append(intermediate, kv)
	}
	return intermediate, nil
}

func Readfile(filename string) (string, error) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return "", err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return "", err
	}
	file.Close()
	return string(content), nil
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
