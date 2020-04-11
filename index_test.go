package bladedb

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func BenchmarkHash(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Hash([]byte(fmt.Sprintf("key_%d", i)))
	}
}

func BenchmarkIndexSet(b *testing.B) {
	index := NewIndex()
	for i := 0; i < b.N; i++ {
		indexRec := IndexRec{
			SSTRecOffset:  uint32(i),
			SSTFileSeqNum: uint32(i),
			TS:            uint64(time.Now().UnixNano() / 1000),
		}
		arr := [32]byte{}
		copy(arr[:], bytes.Repeat([]byte(string(i)), 32))
		index.Set(arr, indexRec)
	}
}

var pool = sync.Pool{New: func() interface{} { return IndexRec{} }}

func BenchmarkIndexSetWithPool(b *testing.B) {
	index := NewIndex()
	for i := 0; i < b.N; i++ {
		indexRec := pool.Get().(IndexRec)
		indexRec.SSTRecOffset = uint32(i)
		indexRec.SSTFileSeqNum = uint32(i)
		indexRec.TS = uint64(time.Now().UnixNano() / 1000)
		arr := [32]byte{}
		copy(arr[:], bytes.Repeat([]byte(string(i)), 32))
		index.Set(arr, indexRec)
		pool.Put(indexRec)
	}
}

func TestIndexSetGet(t *testing.T) {
	checkEqualIndexRec := func(rec1 IndexRec, rec2 IndexRec) bool {
		return rec1.TS == rec2.TS && rec1.SSTFileSeqNum == rec2.SSTFileSeqNum && rec1.SSTRecOffset == rec2.SSTRecOffset
	}
	index := NewIndex()

	indexRecs := make(map[string]IndexRec, 100)

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("keykeykeykeykeykeykeykeykeykeykeykeykeykeykeykeykeykeykeykeykeykeykeykeykeykeykeykeykeykeykeykeykeykey_%d", i)
		indexRec := IndexRec{
			SSTRecOffset:  uint32(i),
			SSTFileSeqNum: uint32(i),
			TS:            uint64(time.Now().UnixNano() / 1000),
		}
		hash := Hash([]byte(key))
		indexRecs[key] = indexRec
		index.Set(hash, indexRec)
	}

	for key, rec := range indexRecs {
		get, _ := index.Get(Hash([]byte(key)))
		require.True(t, checkEqualIndexRec(get, rec), "get result should be same as insert rec")
	}
}

func TestIndexSet_Parallel_SameKey(t *testing.T) {
	index := NewIndex()
	nWrite := 5000000
	wg := sync.WaitGroup{}
	wg.Add(nWrite)

	var latestTime uint64 = 0
	maxFinder := func(v1 uint64, v2 uint64) uint64 {
		if v1 > v2 {
			return v1
		}
		return v2
	}

	for i := 0; i < nWrite; i++ {
		j := i
		go func(k int) {
			defer wg.Done()

			indexRec := IndexRec{
				SSTRecOffset:  uint32(k),
				SSTFileSeqNum: uint32(k),
				TS:            uint64(time.Now().UnixNano() / 1000),
			}

			index.Set(Hash([]byte("1")), indexRec)
			latestTime = maxFinder(latestTime, indexRec.TS)
		}(j)
	}

	wg.Wait()

	rec, _ := index.Get(Hash([]byte("1")))
	require.True(t, rec.TS == latestTime,
		"latest time is not matching with index rec")
}
