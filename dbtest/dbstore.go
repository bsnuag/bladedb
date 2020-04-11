package main

import (
	"bladedb"
	"bytes"
	"fmt"
	"sync"
	"time"
)

var start = time.Now()

func main() {

	err := bladedb.Open()
	if err != nil {
		panic(fmt.Sprintf("error in init() of dbstore"))
	}

	nWrite := 100000000 / 2
	wg := sync.WaitGroup{}

	wg.Add(nWrite)
	writeRecs(nWrite, &wg)
	wg.Wait()

	//wg.Add(4)
	//deleteRecs(4, &wg)
	//wg.Wait()
	//
	//wg.Add(nWrite)
	//readRecs(nWrite, &wg)
	//wg.Wait()

	fmt.Println("TotalTime Before flushing..(sec): ", time.Since(start).Seconds())
	fmt.Println("All Write completed..Flushing db")

	bladedb.Flush()
	//	bladedb.PrintPartitionStats()
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
			key:   bytes.Repeat([]byte(fmt.Sprintf("%d", i)), 22*1)[:11],
			value: bytes.Repeat([]byte(fmt.Sprintf("%d", i)), 128*1)[:128],
			wg:    wg,
		}
		if i%100000 == 0 {
			fmt.Println(fmt.Sprintf("Write completed: %d, time took: %f, total time: %f", i,
				time.Since(localStart).Seconds(), time.Since(start).Seconds()))
			localStart = time.Now()
		}
	}
	//for i := 0; i < nWrite; i++ {
	//	j:=i
	//	go func(k int) {
	//		//bladedb.Put(bytes.Repeat([]byte(fmt.Sprintf("%d", k)), 128),
	//		//	bytes.Repeat([]byte(fmt.Sprintf("%d", k)), 512))
	//		bladedb.Put(bytes.Repeat([]byte("1"), 22*1),
	//			bytes.Repeat([]byte("2"), 128*1))
	//		wg.Done()
	//	}(j)
	//}
	fmt.Println("Push Done....")
}

func doWrite(tempChan chan *Temp) {
	for temp := range tempChan {
		bladedb.Put(temp.key, temp.value)
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
		value,_ := bladedb.Get(temp.key)
		fmt.Printf("Read Key: %s, Value: %s \n", temp.key, string(value))
		//if value == nil {
		//} else if value != nil {
		//	fmt.Printf("Read Key: %s, Value: %s ", temp.key, string(value))
		//}
		temp.wg.Done()
	}
}*/
