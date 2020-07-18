package main

import (
	proto "bladedb/proto"
	"context"
	"fmt"
	"google.golang.org/grpc"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

var keyFormat = "Key_%d"
var valueFormat = "Value_%d"
var wg sync.WaitGroup
var requestCompleted int64
var reportStatusOnBatch = 10000
var noOfWrite = 0
var noOfClient = 0
var startTime = time.Now()

func main() {
	runtime.GOMAXPROCS(100)
	noOfClient = 100
	noOfWrite = 1000
	writePerClient := noOfWrite / noOfClient
	start := 0
	wg.Add(noOfClient)
	for i := 0; i < noOfClient; i++ {
		go loadData(start, start+writePerClient, fmt.Sprintf("client-%d", i))
		start = start + writePerClient
	}
	wg.Wait()
	fmt.Println("total execution time ", time.Since(startTime).Seconds())
}

func loadData(start int, end int, name string) {
	defer wg.Done()
	client := newClient()
	reqCount := 0
	wgNew := sync.WaitGroup{}
	wgNew.Add(end - start)

	for i := start; i < end; i++ {
		reqCount++

		go func(j int) {
			key := fmt.Sprintf(keyFormat, j)
			value := fmt.Sprintf(valueFormat, j)
			sreq := &proto.SetRequest{Key: key, Value: []byte(value)}
			_, err := client.Set(context.Background(), sreq)
			if err != nil {
				fmt.Println(err)
				panic(err)
			}
			wgNew.Done()
		}(i)

		if reqCount == reportStatusOnBatch {
			reportStatus(int64(reqCount))
			reqCount = 0
		}
	}
	if reqCount > 0 {
		reportStatus(int64(reqCount))
	}
	wgNew.Wait()
	fmt.Println(name, " completed")
}

func newClient() proto.BladeDBClient {
	address := fmt.Sprintf("0.0.0.0:%d", "9099")
	client, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}

	dbClient := proto.NewBladeDBClient(client)
	return dbClient
}

func reportStatus(count int64) {
	atomic.AddInt64(&requestCompleted, count)
	fmt.Println(fmt.Sprintf("Completed %d requests, pending requests: %d, time elapsed: %v",
		requestCompleted, int64(noOfWrite)-requestCompleted, time.Since(startTime).Seconds()))
}
