package memstore

import (
	"bladedb"
	"fmt"
	"runtime"
	"testing"
	"time"
)

var memTable, _ = NewMemStore(0)

func BenchmarkMemStoreInsert(b *testing.B) {
	//memTable, _ := NewMemStore(0)
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		ts := time.Now().Unix()
		memTable.Insert(key, value, ts, bladedb.WriteReq)
	}
}

//func BenchmarkMemStoreGet(b *testing.B) {
//	//memTable, _ := NewMemStore(0)
//	//for i := 0; i < b.N; i++ {
//	//	key := []byte(fmt.Sprintf("key%d", i))
//	//	value := []byte(fmt.Sprintf("value-%d", i))
//	//	ts := time.Now().Unix()
//	//	memTable.Insert(key, value, ts)
//	//}
//
//	for i := 0; i < b.N; i++ {
//		key := []byte(fmt.Sprintf("key%d", i))
//		val, _ := memTable.Find(key)
//		fmt.Sprintf("%v",val)
//	}
//}

func BenchmarkMemStoreGetGoRoutine(b *testing.B) {
	//memTable, _ := NewMemStore(0)
	//for i := 0; i < b.N; i++ {
	//	key := []byte(fmt.Sprintf("key%d", i))
	//	value := []byte(fmt.Sprintf("value-%d", i))
	//	ts := time.Now().Unix()
	//	memTable.Insert(key, value, ts)
	//}
	var count int64 = 0
	runtime.GOMAXPROCS(runtime.NumCPU())
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		go func() {
			val, _ := memTable.Find(key)
			if val!=nil {
				count++
			}
			fmt.Sprintf("%v",val)
		}()
	}
	//fmt.Println(count)
}