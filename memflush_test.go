package bladedb

import (
	"bladedb/memstore"
	"bladedb/sklist"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"
)

func TestWriteSSTAndIndex(t *testing.T) {
	partitionId := 0
	dir, err := ioutil.TempDir("", "memFlushTest")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	SSTDir = dir
	partitionInfoMap[partitionId] = &PartitionInfo{
		partitionId:  partitionId,
		index:        sklist.New(),
		levelsInfo:   newLevelInfo(),
		sstReaderMap: make(map[uint32]SSTReader),
		sstSeq:       0,
		walSeq:       0,
	}

	memTable_1, _ := memstore.NewMemStore(partitionId)
	for i := 0; i < 20; i++ {
		time.Sleep(time.Nanosecond * 10)
		key := fmt.Sprintf("%dKey_", i)
		value := fmt.Sprintf("%dValue_", i)
		memTable_1.Insert([]byte(key), []byte(value), time.Now().UnixNano(), DefaultConstants.writeReq)
	}


}
