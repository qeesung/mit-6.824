package mapreduce

import (
	"fmt"
	"log"
	"net/rpc"
	"strconv"
	"sync"
	"sync/atomic"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	taskChan := make(chan string)
	go func() {
		// Publish the task to the task channel
		switch phase {
		case mapPhase:
			for _, mapFile := range mapFiles {
				taskChan <- mapFile
			}
		case reducePhase:
			for i := 0; i < nReduce; i++ {
				taskChan <- strconv.Itoa(i)
			}
		}
		close(taskChan) // all task scheduled
	}()

	var taskIndex int32
	wg := sync.WaitGroup{}
	wg.Add(ntasks)
	go func() { // loop the workers
		for workerAddress := range registerChan {
			wa := workerAddress
			go func() { // rpc call, schedule the task to worker
				workerClient, err := rpc.Dial("unix", wa)
				if err != nil {
					log.Fatal(err.Error())
				}
				for task := range taskChan {
					var taskArg DoTaskArgs
					switch phase {
					case mapPhase:
						taskArg = DoTaskArgs{
							JobName:       jobName,
							File:          task,
							Phase:         phase,
							TaskNumber:    int(taskIndex),
							NumOtherPhase: n_other,
						}
					case reducePhase:
						reduceNumber, _ := strconv.Atoi(task)
						taskArg = DoTaskArgs{
							JobName:       jobName,
							Phase:         phase,
							TaskNumber:    reduceNumber,
							NumOtherPhase: n_other,
						}
					}
					atomic.AddInt32(&taskIndex, 1)
					err = workerClient.Call("Worker.DoTask", taskArg, nil)
					if err != nil {
						log.Fatal(err.Error())
					}
					wg.Done()
				}
			}()
		}
	}()
	wg.Wait()

	fmt.Printf("Schedule: %v done\n", phase)
}
