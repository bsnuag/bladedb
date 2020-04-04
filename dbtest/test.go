package main

import (
	"bladedb"
	"bladedb/index"
	"bladedb/prototest"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/niubaoshu/gotiny"
	"runtime"
	"sync"
	"time"
)

func main() {
	//Value_Serialization_Struct_Type()
	Value_No_Serialization_Struct_Type()
	//
	//Value_Serialization_Proto_Type()
	//Value_No_Serialization_Proto_Type()
	//indexTest()
}
func indexTest() {
	s := time.Now()
	PrintMemUsage()

	idx := index.NewIndex()
	var n int64 = 30000000
	var i int64 = 0
	for ; i < n; i++ {
		indexRec := index.IndexRec{
			SSTRecOffset:  uint32(i),
			SSTFileSeqNum: uint32(i),
			TS:            bladedb.NanoTime(),
		}
		idx.Set(fmt.Sprintf("%d", i), indexRec)
	}
	PrintMemUsage()
	fmt.Println(time.Since(s).Seconds())
	runtime.GC()
	PrintMemUsage()

	s = time.Now()
	//index read
	//for ; i < n; i++ {
	//	key := fmt.Sprintf("%d", i)
	//	get := idx.Get(key).Value()
	//	fmt.Sprintf("%v", get)
	//}
	fmt.Println(time.Since(s).Seconds())

}

type Rec struct {
	Offset int64
	Fnum   int64
	Ts     int64
}

func Value_Serialization_Struct_Type() {
	fmt.Println("Value_Serialization_Struct_Type")
	s := time.Now()
	PrintMemUsage()
	var n int64 = 30000000
	maps := make(map[string][]byte, n)
	var i int64 = 0
	for ; i < n; i++ {
		//rec := Rec{
		//	Offset: i,
		//	Fnum:   i,
		//	Ts:     time.Now().UnixNano(),
		//}
		marshal := gotiny.Marshal(&Rec{
			Offset: i,
			Fnum:   i,
			Ts:     time.Now().UnixNano(),
		})
		maps[fmt.Sprintf("%d", i)] = marshal
	}
	PrintMemUsage()
	runtime.GC()
	PrintMemUsage()
	fmt.Println(time.Since(s).Seconds())
}

func Value_No_Serialization_Struct_Type() {
	fmt.Println("Value_No_Serialization_Struct_Type")
	s := time.Now()
	PrintMemUsage()
	mutex := sync.Mutex{}

	var n int64 = 30000000
	maps := make(map[string]*Rec, n)
	var i int64 = 0
	for ; i < n; i++ {
		rec := Rec{
			Offset: i,
			Fnum:   i,
			Ts:     time.Now().UnixNano(),
		}
		mutex.Lock()
		maps[fmt.Sprintf("%d", i)] = &rec
		mutex.Unlock()
	}
	PrintMemUsage()
	runtime.GC()
	PrintMemUsage()
	fmt.Println(time.Since(s).Seconds())

	s = time.Now()
	//index read
	for ; i < n; i++ {
		key := fmt.Sprintf("%d", i)
		rec := maps[key]
		fmt.Sprintf("%v", rec)
	}
	fmt.Println(time.Since(s).Seconds())
}

func Value_Serialization_Proto_Type() {
	fmt.Println("Value_Serialization_Proto_Type")
	s := time.Now()
	PrintMemUsage()
	var n int64 = 10000000
	maps := make(map[string][]byte, n)
	var i int64 = 0
	for ; i < n; i++ {
		rec := test.Rec{
			Offset: i,
			Fnum:   i,
			Ts:     time.Now().UnixNano(),
		}
		bytes, _ := proto.Marshal(&rec)
		maps[fmt.Sprintf("%d", i)] = bytes
	}
	PrintMemUsage()

	runtime.GC()
	PrintMemUsage()
	fmt.Println(time.Since(s).Seconds())
}

func Value_No_Serialization_Proto_Type() {
	fmt.Println("Value_No_Serialization_Proto_Type")
	s := time.Now()
	PrintMemUsage()
	var n int64 = 10000000
	maps := make(map[string]*test.Rec, n)
	var i int64 = 0
	for ; i < n; i++ {
		rec := test.Rec{
			Offset: i,
			Fnum:   i,
			Ts:     time.Now().UnixNano(),
		}
		maps[fmt.Sprintf("%d", i)] = &rec
	}
	PrintMemUsage()

	runtime.GC()
	PrintMemUsage()
	fmt.Println(time.Since(s).Seconds())
}

func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
