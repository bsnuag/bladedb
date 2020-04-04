package bladedb

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var compactTaskQueue chan *CompactInfo = nil //TODO - change channel type to compactTask with pId and thisLevel
var compactSubscriber sync.WaitGroup
var compactActive int32 = 0 //0- false, 1 - true

func activateCompactWorkers() {
	if DefaultConstants.compactWorker != 0 {
		compactTaskQueue = make(chan *CompactInfo, 10000)
		compactActive = 1
	}
	for i := 1; i <= DefaultConstants.compactWorker; i++ {
		go compactWorker(fmt.Sprintf("CompactWorker- %d", i))
	}
}

//blocks from accepting new compact tasks, but waits to complete already submitted tasks
func stopCompactWorker() {
	if DefaultConstants.compactWorker == 0 {
		return
	}
	s := time.Now()
	fmt.Println("Request received to stop compact workers")
	atomic.CompareAndSwapInt32(&compactActive, 1, 0)
	close(compactTaskQueue)
	fmt.Println("No new compaction task would be taken, already published tasks will be completed")

	fmt.Println("Waiting for all submitted compaction tasks to be completed")
	compactSubscriber.Wait()
	fmt.Println("all submitted compaction tasks completed, time taken(Sec): ", time.Since(s).Seconds())
}

//publish new task to compact queue if compaction is active(compactActive==true)
func publishCompactTask(compactTask *CompactInfo) {
	if isCompactionActive() {
		compactTaskQueue <- compactTask
	} else {
		fmt.Println("Compaction is not active, cannot publish new task")
	}
}

func isCompactionActive() bool {
	return atomic.LoadInt32(&compactActive) == 1
}
