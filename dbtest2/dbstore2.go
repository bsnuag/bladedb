package main

import (
	"bladedb"
	"fmt"
	"runtime"
	"sync"
	"time"
)

//rad-write-delete in concurrent
func main() {

	err := bladedb.PreparePartitionIdsMap()
	if err != nil {
		panic(fmt.Sprintf("error in init() of dbstore"))
	}

	start := time.Now().UnixNano()

	totalOps := 25 //10+10+5

	wg := sync.WaitGroup{}
	wg.Add(totalOps)

	writeRecs(10, &wg)
	deleteRecs(5, &wg)
	readRecs(10, &wg)

	wg.Wait()

	fmt.Println("TotalTime Before flushing..(ns): ", (time.Now().UnixNano() - start))
	fmt.Println("All Write completed..Flushing db")
	//bladedb.Drain()

	bladedb.Flush()
	bladedb.MemFlushQueueWG.Wait()
	bladedb.PrintPartitionStats()
	fmt.Println("TotalTime After Flusing (ns): ", (time.Now().UnixNano() - start))
}


type Temp struct {
	key   string
	value string
	wg    *sync.WaitGroup
}

func writeRecs(nWrite int, wg *sync.WaitGroup) {
	tChan := make(chan *Temp, 100000)
	for i := 0; i < runtime.NumCPU()*4; i++ {
		go doWrite(tChan)
	}

	for i := 0; i < nWrite; i++ {
		j := i
		//j := 101 + i
		tChan <- &Temp{
			key:   fmt.Sprintf("Key%d", j),
			value: fmt.Sprintf("Value%d", j),
			wg:    wg,
		}
	}
	fmt.Println("Push Done....")
}

func doWrite(tempChan chan *Temp) {
	for temp := range tempChan {
		write(temp.key, temp.value)
		temp.wg.Done()
	}
}

func write(key string, value string) {
	ts := time.Now().Unix()
	bladedb.Put(key, value, ts)
}

func deleteRecs(nDelete int, wg *sync.WaitGroup) {
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
		ts := time.Now().Unix()
		bladedb.Remove(temp.key, ts)
		temp.wg.Done()
	}
}

func readRecs(nRead int, wg *sync.WaitGroup) {
	tChan := make(chan *Temp, 100000)
	for i := 0; i < runtime.NumCPU()*4; i++ {
		go doRead(tChan)
	}

	for i := 0; i < nRead; i++ {
		j := i //j := 101 + i
		tChan <- &Temp{
			key:   fmt.Sprintf("Key%d", j),
			value: "",
			wg:    wg,
		}
	}

}

func doRead(tempChan chan *Temp) {
	for temp := range tempChan {
		value := bladedb.Get(temp.key)
		fmt.Printf("Read Key: %s, Value: %s \n", temp.key, string(value))
		//if value == nil {
		//} else if value != nil {
		//	fmt.Printf("Read Key: %s, Value: %s ", temp.key, string(value))
		//}
		temp.wg.Done()
	}
}