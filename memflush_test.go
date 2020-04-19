package bladedb

import (
	"bladedb/memstore"
	"fmt"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

func setupMemflushTest(partitionId int) (cFileName string, tearFun func()) {

	config := Config{LogFileMaxLen: DefaultLogFileMaxLen,
		LogDir:              "memFlushTestLog/",
		DataDir:             "memFlushTestSST/",
		WalFlushPeriodInSec: 10,
		CompactWorker:       0,
		NoOfPartitions:      1,
		MemFlushWorker:      3,
	}
	bytes, _ := yaml.Marshal(&config)
	cFile, err := ioutil.TempFile("", "abcdef.yaml")
	if err != nil {
		panic(err)
	}
	if _, err = cFile.Write(bytes); err != nil {
		panic(err)
	}
	if err = cFile.Close(); err != nil {
		panic(err)
	}

	return cFile.Name(), func() {
		os.RemoveAll(config.DataDir)
		os.RemoveAll(config.LogDir)
		os.RemoveAll(cFile.Name())
		db = nil
	}
}

func TestMemFlushWriteSSTAndIndex_Overlap_Writes(t *testing.T) {
	partitionId := 0
	cFileName, tearFun := setupMemflushTest(partitionId)
	defer tearFun()
	defer Drain()
	Open(cFileName)

	//3 memtable with same recs with different timestamp
	iLog1 := db.pMap[partitionId].prepareMemFlushInput(0, 900000, WriteReq, 1)
	iLog2 := db.pMap[partitionId].prepareMemFlushInput(0, 400000, WriteReq, 2)
	iLog3 := db.pMap[partitionId].prepareMemFlushInput(0, 500000, WriteReq, 3)

	//add iLogs to pInfo
	db.pMap[partitionId].inactiveLogDetails = []*InactiveLogDetails{&iLog1, &iLog2, &iLog3}

	//push it to memflush queue
	publishMemFlushTask(&iLog1)
	publishMemFlushTask(&iLog2)
	publishMemFlushTask(&iLog3)
	time.Sleep(5 * time.Second) //for worker to pickup task before calling stop

	ts3Count, ts2Count, ts1Count, othersTsCount := 0, 0, 0, 0
	for _, value := range db.pMap[partitionId].index.index {
		if value.TS == 3 {
			ts3Count++
		} else if value.TS == 1 {
			ts1Count++
		} else if value.TS == 2 {
			ts2Count++
		} else {
			othersTsCount++
		}
	}

	//validate index timestamps
	require.True(t, ts3Count == 500000)
	require.True(t, ts1Count == 400000)
	require.True(t, ts2Count == 0)
	require.True(t, othersTsCount == 0)

	//validate index rec count
	require.True(t, db.pMap[partitionId].index.Size() == 900000)

	//validate inactive logs rec count
	require.True(t, len(db.pMap[partitionId].inactiveLogDetails) == 0)

	//validate manifest
	err, manifest := replayManifest()
	if err!=nil{
		panic(err)
	}
	require.True(t, len(manifest.sstManifest[partitionId].manifestRecs) == 3)
	require.True(t, len(manifest.logManifest[partitionId].manifestRecs) == 4) //default + 3 created manually
}

func TestMemFlushWriteSSTAndIndex_Overlap_Writes_Deletes(t *testing.T) {
	partitionId := 0
	cFileName, tearFun := setupMemflushTest(partitionId)
	defer tearFun()
	defer Drain()
	Open(cFileName)

	//3 memtable with same recs with different timestamp
	iLog1 := db.pMap[partitionId].prepareMemFlushInput(0, 100000, WriteReq, 1)
	iLog2 := db.pMap[partitionId].prepareMemFlushInput(0, 100000, DelReq, 2)
	iLog3 := db.pMap[partitionId].prepareMemFlushInput(50000, 100000, WriteReq, 3)

	//add iLogs to pInfo
	db.pMap[partitionId].inactiveLogDetails = []*InactiveLogDetails{&iLog1, &iLog2, &iLog3}

	//push it to memflush queue
	publishMemFlushTask(&iLog1)
	publishMemFlushTask(&iLog2)
	publishMemFlushTask(&iLog3)
	time.Sleep(5 * time.Second) //for worker to pickup task before calling stop

	ts3Count, ts2Count, ts1Count, othersCount := 0, 0, 0, 0
	for _, value := range db.pMap[partitionId].index.index {
		if value.TS == 3 {
			ts3Count++
		} else if value.TS == 1 {
			ts1Count++
		} else if value.TS == 2 {
			ts2Count++
		} else {
			othersCount++
		}
	}

	//validate index timestamps
	require.True(t, ts3Count == 50000)
	require.True(t, ts1Count == 50000)
	require.True(t, ts2Count == 0)
	require.True(t, othersCount == 0)

	//validate index rec count -
	// memflush doesn't have effect on index for delete requests (gets removed while serving actual request)
	require.True(t, db.pMap[partitionId].index.Size() == 100000)

	//validate inactive logs rec count
	require.True(t, len(db.pMap[partitionId].inactiveLogDetails) == 0)

	//validate manifest
	err, manifest := replayManifest()
	if err!=nil{
		panic(err)
	}
	require.True(t, len(manifest.sstManifest[partitionId].manifestRecs) == 3)
	require.True(t, len(manifest.logManifest[partitionId].manifestRecs) == 4)
}

func TestMemFlushWriteSSTAndIndex_SSTMeta(t *testing.T) {
	partitionId := 0
	cFileName, tearFun := setupMemflushTest(partitionId)
	defer tearFun()
	defer Drain()
	Open(cFileName)

	iLog1 := db.pMap[partitionId].prepareMemFlushInput(0, 100000, WriteReq, 1)
	//mark one key as delete
	iLog1.MemTable.Insert([]byte("99Key_"), []byte(""), 1, DelReq)

	seqNum, _ := db.pMap[partitionId].writeSSTAndIndex(iLog1.MemTable.Recs())

	//very results..
	require.True(t, 1 == len(db.pMap[partitionId].levelsInfo[0].sstSeqNums))
	require.NotNil(t, db.pMap[partitionId].levelsInfo[0].sstSeqNums[seqNum])
	require.True(t, db.pMap[partitionId].sstReaderMap[seqNum].noOfWriteReq == 99999)
	require.True(t, db.pMap[partitionId].sstReaderMap[seqNum].noOfDelReq == 1)
	require.True(t, string(db.pMap[partitionId].sstReaderMap[seqNum].startKey) == "0Key_")
	require.True(t, string(db.pMap[partitionId].sstReaderMap[seqNum].endKey) == "9Key_")
}

func (pInfo *PartitionInfo) prepareMemFlushInput(start int, end int, reqType byte, ts uint64) InactiveLogDetails {
	//Write data into mem and then flush it to sst
	memTable := memstore.NewMemStore()

	for i := start; i < end; i++ {
		key := fmt.Sprintf("%dKey_", i)
		value := fmt.Sprintf("%dValue_", i)
		memTable.Insert([]byte(key), []byte(value), ts, reqType)
	}

	return InactiveLogDetails{
		FileName:    "NA",
		FileSeqNum:  pInfo.getNextLogSeq(),
		WriteOffset: 0,
		MemTable:    memTable,
		PartitionId: pInfo.partitionId,
	}
}
