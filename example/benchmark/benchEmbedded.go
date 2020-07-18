package main

import (
	"bladedb"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	nThreads := flag.Int("nThreads", 8, "number of clients")

	nWrite := flag.Int64("nW", 40000000, "number of writes")
	nRead := flag.Int64("nR", 40000000, "number of reads")

	readOnly := flag.Bool("r", false, "simulate read only")
	writeOnly := flag.Bool("w", false, "simulate write only")
	readWrite := flag.Bool("rw", false, "simulate read write concurrently")

	keySz := flag.Int("kSz", 256, "key size in bytes")
	valSz := flag.Int("vSz", 256, "value size in bytes")

	flag.Parse()

	configPath, _ := filepath.Abs("conf/config.yaml")
	_, err := os.Stat(configPath)
	if os.IsNotExist(err) {
		log.Fatalf(fmt.Sprintf("missing default config file: %s", configPath))
	}
	bladedb.Open(configPath)

	start := time.Now()
	readTaskWG := sync.WaitGroup{}
	writeTaskWG := sync.WaitGroup{}

	if *writeOnly { //only write
		benchStats.writeStats = &WriteStats{}
		writeTaskWG.Add(int(*nWrite))
		simulateWrite(&writeTaskWG, nWrite, *nThreads, keySz, valSz) //blocking call
	} else if *readOnly { //only read
		benchStats.readStats = &ReadStats{}
		readTaskWG.Add(int(*nRead))
		simulateRead(&readTaskWG, nRead, *nThreads, keySz) //blocking call
	} else if *readWrite { //read write happens in parallel, threads divided equally for read, write
		benchStats.writeStats = &WriteStats{}
		benchStats.readStats = &ReadStats{}

		readTaskWG.Add(int(*nRead))
		writeTaskWG.Add(int(*nWrite))

		go simulateWrite(&writeTaskWG, nWrite, *nThreads/2, keySz, valSz)
		simulateRead(&readTaskWG, nRead, *nThreads/2, keySz)
	} else {
		log.Fatal("One of operation (read/write/read-write) must be true")
	}

	benchStats.duration = time.Since(start).Seconds()

	drainStart := time.Now()
	bladedb.Drain()
	benchStats.drainDuration = time.Since(drainStart).Seconds()

	printBenchStats()
}

func simulateWrite(wg *sync.WaitGroup, nWrite *int64, nThreads int, keySz *int, valSz *int) {
	benchStats.writeStats.nWrite = *nWrite
	tChan := make(chan *HelperChan, (*nWrite*50)/100) //channel would be 50% of total write
	for i := 0; i < nThreads; i++ {
		go write(tChan)
	}
	startTime := time.Now()
	localStart := time.Now()
	var i int64 = 0
	for ; i < *nWrite; i++ {
		tChan <- &HelperChan{
			key:   []byte(fmt.Sprintf("%0*d", *keySz, i)),
			value: []byte(fmt.Sprintf("%0*d", *valSz, i+1)), //to not have same value for Key & Value :)
			wg:    wg,
		}
		if i > 0 && i%100000 == 0 {
			fmt.Println(fmt.Sprintf("Write(100k) completed: %d, duration: %f, total duration: %f", i,
				time.Since(localStart).Seconds(), time.Since(startTime).Seconds()))
			localStart = time.Now()
		}
	}
	if i > 0 && i%100000 == 0 {
		fmt.Println(fmt.Sprintf("Write(100k) completed: %d, duration: %f, total duration: %f", i,
			time.Since(localStart).Seconds(), time.Since(startTime).Seconds()))
		localStart = time.Now()
	}
	wg.Wait()
	benchStats.writeStats.ops = int64(math.Round((float64(*nWrite)) / time.Since(startTime).Seconds()))
}

func write(wChan chan *HelperChan) {
	for temp := range wChan {
		err := bladedb.Put(temp.key, temp.value)
		if err != nil {
			atomic.AddUint32(&benchStats.writeStats.errored, 1)
			fmt.Println(fmt.Sprintf("Error while writing key: %s, error: %v", temp.key, err))
		} else {
			atomic.AddUint32(&benchStats.writeStats.success, 1)
		}
		temp.wg.Done()
	}
}

func simulateRead(wg *sync.WaitGroup, nRead *int64, nThreads int, keySz *int) {
	benchStats.readStats.nRead = *nRead
	tChan := make(chan *HelperChan, (*nRead*50)/100) //channel would be 50% of total write
	for i := 0; i < nThreads; i++ {
		go read(tChan)
	}
	startTime := time.Now()
	localStart := time.Now()
	var i int64 = 0
	for ; i < *nRead; i++ {
		tChan <- &HelperChan{
			key: []byte(fmt.Sprintf("%0*d", *keySz, i)),
			wg:  wg,
		}
		if i > 0 && i%100000 == 0 {
			fmt.Println(fmt.Sprintf("Read(100k) completed: %d, duration: %f, total duration: %f", i,
				time.Since(localStart).Seconds(), time.Since(startTime).Seconds()))
			localStart = time.Now()
		}
	}
	if i > 0 && i%100000 == 0 {
		fmt.Println(fmt.Sprintf("Read(100k) completed: %d, duration: %f, total duration: %f", i,
			time.Since(localStart).Seconds(), time.Since(startTime).Seconds()))
		localStart = time.Now()
	}
	wg.Wait()
	benchStats.readStats.ops = int64(math.Round((float64(*nRead)) / time.Since(startTime).Seconds()))
}

func read(tempChan chan *HelperChan) {
	for temp := range tempChan {
		value, err := bladedb.Get(temp.key)
		if err != nil {
			atomic.AddUint32(&benchStats.readStats.errored, 1)
			fmt.Println(fmt.Sprintf("Error while reading key: %s, error: %v", temp.key, err))
		} else if value == nil {
			atomic.AddUint32(&benchStats.readStats.notFound, 1)
		} else {
			atomic.AddUint32(&benchStats.readStats.found, 1)
		}
		temp.wg.Done()
	}
}

func printBenchStats() {
	fmt.Println(fmt.Sprintf("Total op duration (Seconds): %f", benchStats.duration))
	fmt.Println(fmt.Sprintf("Total time to stop db (Seconds): %f", benchStats.drainDuration))

	if benchStats.readStats != nil {
		fmt.Println(fmt.Sprintf("Number of reads: %d, found: %d, not-found: %d, errored: %d",
			benchStats.readStats.nRead, benchStats.readStats.found,
			benchStats.readStats.notFound, benchStats.readStats.errored))

		fmt.Println(fmt.Sprintf("Read ops: %d", benchStats.readStats.ops))
	}

	if benchStats.writeStats != nil {
		fmt.Println(fmt.Sprintf("Number of writes: %d, succeeded: %d errored: %d",
			benchStats.writeStats.nWrite, benchStats.writeStats.success, benchStats.writeStats.errored))
		fmt.Println(fmt.Sprintf("Write ops: %d", benchStats.writeStats.ops))
	}
}

var benchStats = BenchStats{}

type BenchStats struct {
	duration      float64
	drainDuration float64
	writeStats    *WriteStats
	readStats     *ReadStats
}

type WriteStats struct {
	nWrite  int64
	errored uint32
	success uint32
	ops     int64
}

type ReadStats struct {
	nRead    int64
	found    uint32
	notFound uint32
	errored  uint32
	ops      int64
}

type HelperChan struct {
	key   []byte
	value []byte
	wg    *sync.WaitGroup
}

//Pending Itesm:
//2. stats for benchmark - ops/sec, latency, memory, disk
//3. document this part
