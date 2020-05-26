package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type EmptyRep struct{}
type EmptyReq struct{}

type RegisterRep struct {
	Id      int
	NReduce int
}

type StatusReportReq struct {
	Task   *Task
	Done   bool
	Failed bool
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
