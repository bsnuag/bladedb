package memstore

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestNewMemStoreCreate(t *testing.T) {
	memStore := NewMemStore()
	if memStore == nil {
		panic("Error while creating new memstore")
	}
}

func TestMemStoreInsert(t *testing.T) {
	memStore := NewMemStore()
	if memStore == nil {
		panic("Error while creating new memstore")
		return
	}
	N := 1000000
	for i := 0; i < N; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		ts := uint64(time.Now().UnixNano() / 1000)
		memStore.Insert(key, value, ts, 0)
	}
	assert.True(t, memStore.list.Length == N, "Size should be 1000000")
}

func TestMemStoreFind(t *testing.T) {
	memStore := NewMemStore()
	if memStore == nil {
		panic("Error while creating new memstore")
	}
	//insert
	N := 100
	for i := 0; i < N; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		ts := uint64(time.Now().UnixNano() / 1000)
		memStore.Insert(key, value, ts, 0)
	}
	findCount := 0
	for i := 0; i < N; i++ {
		keyString := fmt.Sprintf("key-%d", i)
		key := []byte(keyString)
		_, err := memStore.Find(key)
		if err == nil {
			findCount++
			//fmt.Println(value)
		} else {
			//fmt.Println(fmt.Sprintf("couldn't find key: %s", keyString))
		}
	}
	assert.True(t, memStore.list.Length == findCount, "Size should be 100")
}

func BenchmarkMemStoreInsert(b *testing.B) {
	memTable := NewMemStore()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		ts := uint64(time.Now().UnixNano() / 1000)
		memTable.Insert(key, value, ts, 0)
	}
}
