package main

import (
	"bladedb"
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var start = time.Now()

func main() {
	bladedb.Open()

	nWrite := 10000000
	//nWrite := 100000000 / 2
	wg := sync.WaitGroup{}

	//wg.Add(nWrite)
	//writeRecs(nWrite, &wg)

	//wg.Add(4)
	//deleteRecs(4, &wg)
	//wg.Wait()
	//
	wg.Add(nWrite)
	readRecs(nWrite, &wg)

	fmt.Println("TotalTime Before flushing..(sec): ", time.Since(start).Seconds())
	fmt.Println("All Write completed..Flushing db")

	bladedb.Drain()
	//bladedb.PrintPartitionStats()
	fmt.Println("TotalTime After Flusing (ns): ", time.Since(start).Seconds())

	/*	fmt.Println("Starting up again...\n 1.Keys will be dumped to WAL and MEMTable from Unclosed WAL File \n 2. Keys will be loaded from SST to Index")
		err1 := bladedb.PreparePartitionIdsMap()
		if err1 != nil {
			panic(fmt.Sprintf("error in init() of dbstore"))
		}
		bladedb.PrintPartitionStats()

		bladedb.Drain()
	*/
}

type Temp struct {
	key   []byte
	value []byte
	wg    *sync.WaitGroup
}

func writeRecs(nWrite int, wg *sync.WaitGroup) {
	tChan := make(chan *Temp, 50000000)
	for i := 0; i < 32; i++ {
		go doWrite(tChan)
	}
	start = time.Now()
	localStart := time.Now()
	for i := 0; i < nWrite; i++ {
		tChan <- &Temp{
			key:   bytes.Repeat([]byte(fmt.Sprintf("%d", i)), 22*1)[:22],
			value: bytes.Repeat([]byte(fmt.Sprintf("%d", i)), 128*1)[:128],
			wg:    wg,
		}
		if i%100000 == 0 {
			fmt.Println(fmt.Sprintf("Write completed: %d, time took: %f, total time: %f", i,
				time.Since(localStart).Seconds(), time.Since(start).Seconds()))
			localStart = time.Now()
		}
	}
	wg.Wait()
	fmt.Println(fmt.Sprintf("Write Metrics, errored: %d", writeMetrics.err))
}

func doWrite(tempChan chan *Temp) {
	for temp := range tempChan {
		err := bladedb.Put(temp.key, temp.value)
		if err != nil {
			atomic.AddUint32(&writeMetrics.err, 1)
			fmt.Println(fmt.Sprintf("Error while writing key: %s, error: %v", temp.key, err))
		}
		temp.wg.Done()
	}
}

/*func deleteRecs(nDelete int, wg *sync.WaitGroup) {
	tChan := make(chan *Temp, 100000)
	for i := 0; i < runtime.NumCPU()*4; i++ {
		go doWrite(tChan)
	}

	for i := 0; i < nDelete; i++ {
		j := i
		//j := 101 + i
		tChan <- &Temp{
			key:   fmt.Sprintf("Key%d", j),
			value: "",
			wg:    wg,
		}
	}
}

func doDelete(tempChan chan *Temp) {
	for temp := range tempChan {
		bladedb.Remove(temp.key)
		temp.wg.Done()
	}
}*/

func readRecs(nRead int, wg *sync.WaitGroup) {
	tChan := make(chan *Temp, 50000000)
	for i := 0; i < 32; i++ {
		go doRead(tChan)
	}

	for i := 0; i < nRead; i++ {
		tChan <- &Temp{
			key: bytes.Repeat([]byte(fmt.Sprintf("%d", i)), 22*1)[:22],
			wg:  wg,
		}
	}
	wg.Wait()
	fmt.Println(fmt.Sprintf("Read Metrics, found: %d, notFound: %d, errored: %d",
		readMetrics.found, readMetrics.notFound, readMetrics.err))
}

func doRead(tempChan chan *Temp) {
	for temp := range tempChan {
		value, err := bladedb.Get(temp.key)
		if err != nil {
			atomic.AddUint32(&readMetrics.err, 1)
			fmt.Println(fmt.Sprintf("Error while reading key: %s, error: %v", temp.key, err))
		} else if value == nil {
			atomic.AddUint32(&readMetrics.notFound, 1)
		} else if value != nil {
			atomic.AddUint32(&readMetrics.found, 1)
		}
		updateReadCount(1)
		temp.wg.Done()
	}
}

var writeMetrics = WriteMetrics{}

type WriteMetrics struct {
	err uint32
}

var readMetrics = ReadMetrics{}

type ReadMetrics struct {
	found    uint32
	notFound uint32
	err      uint32
}

var metrics = Metrics{}

type Metrics struct {
	rCount uint32
	wCount uint32
	dCount uint32
}

func updateReadCount(n uint32) {
	count := atomic.AddUint32(&metrics.rCount, n)
	if count%100000 == 0 {
		fmt.Println(fmt.Sprintf("Read complted :%d", count))
	}
}
