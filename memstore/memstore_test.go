package memstore

import (
	"bladedb"
	"fmt"
	"testing"
	"time"
)

func TestNewMemStoreCreate(t *testing.T) {
	memStore, err := NewMemStore(0)
	if memStore==nil || err!=nil {
		panic("Error while creating new memstore")
		return
	}
}

func TestMemStoreInsert(t *testing.T) {
	memStore, err := NewMemStore(0)
	if memStore==nil || err!=nil {
		panic("Error while creating new memstore")
		return
	}
	N:=1000000
	for i:=0;i<N;i++{
		key := []byte(fmt.Sprintf("key-%d",i))
		value := []byte(fmt.Sprintf("value-%d",i))
		ts := time.Now().Unix()
		memStore.Insert(key, value, ts, bladedb.WriteReq)
	}
	fmt.Println(memStore.size)
}

func TestMemStoreFind(t *testing.T) {
	memStore, err := NewMemStore(0)
	if memStore==nil || err!=nil {
		panic("Error while creating new memstore")
		return
	}
	//insert
	N:=100
	for i:=0;i<N;i++{
		key := []byte(fmt.Sprintf("key-%d",i))
		value := []byte(fmt.Sprintf("value-%d",i))
		ts := time.Now().Unix()
		memStore.Insert(key, value, ts, bladedb.WriteReq)
	}
	for i:=0;i<N;i++{
		keyString:=fmt.Sprintf("key-%d",i)
		key := []byte(keyString)
		value, err :=memStore.Find(key)
		if err == nil {
			fmt.Println(value)
		} else {
			fmt.Println(fmt.Sprintf("couldn't find key: %s", keyString))
		}
	}
}