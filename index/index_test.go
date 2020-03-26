package index

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func TestIndexSet(t *testing.T) {
	checkEqualIndexRec := func(rec1 IndexRec, rec2 IndexRec) bool {
		return rec1.TS == rec2.TS && rec1.SSTFileSeqNum == rec2.SSTFileSeqNum && rec1.SSTRecOffset == rec2.SSTRecOffset
	}
	index := NewIndex()

	indexRecs := make(map[string]IndexRec, 100)

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key_%d", i)
		indexRec := IndexRec{
			SSTRecOffset:  uint32(i),
			SSTFileSeqNum: uint32(i),
			TS:            time.Now().UnixNano(),
		}
		indexRecs[key] = indexRec
		index.Set(key, indexRec)
	}

	for key, rec := range indexRecs {
		get := index.Get(key)
		require.True(t, checkEqualIndexRec(get.value, rec), "get result should be same as insert rec")
	}
}

func TestIndex_All(t *testing.T) {
	checkEqualIndexRec := func(rec1 IndexRec, rec2 IndexRec) bool {
		return rec1.TS == rec2.TS && rec1.SSTFileSeqNum == rec2.SSTFileSeqNum && rec1.SSTRecOffset == rec2.SSTRecOffset
	}
	index := NewIndex()
	indexRec := IndexRec{
		SSTRecOffset:  0,
		SSTFileSeqNum: 0,
		TS:            time.Now().UnixNano(),
	}
	key := "1"

	set := index.Set(key, indexRec)
	require.True(t, checkEqualIndexRec(set.value, indexRec), "set result should be same as insert rec")

	get := index.Get(key)
	require.True(t, checkEqualIndexRec(get.value, indexRec), "get result should be same as insert rec")

	remove := index.Remove(key)
	require.True(t, checkEqualIndexRec(remove.value, indexRec), "delete result should be same as insert rec")

	deletedGet := index.Get(key)
	require.Nil(t, deletedGet, "get result should be nil after delete")
}

func TestIndexSet_Parallel_SameKey(t *testing.T) {
	index := NewIndex()
	nWrite := 5000000
	wg := sync.WaitGroup{}
	wg.Add(nWrite)

	var latestTime int64 = 0
	maxFinder := func(v1 int64, v2 int64) int64 {
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
				TS:            time.Now().UnixNano(),
			}
			index.Set("1", indexRec)
			latestTime = maxFinder(latestTime, indexRec.TS)
		}(j)
	}

	wg.Wait()

	require.True(t, index.Get("1").Value().TS == latestTime,
		"latest time is not matching with index rec")
}
