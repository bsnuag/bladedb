package bladedb

import (
	"bladedb/memstore"
	"fmt"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"
)

func setupMemflushTest(partitionId int) func() {
	//setup manifest
	tempFile, _ := ioutil.TempFile("", "memflush_test_manifest")
	ManifestFileName = "data/test_manifest"
	ManifestFileName = tempFile.Name()
	if err := initManifest(); err != nil {
		panic(err)
	}

	//setup sst dir
	dir, err := ioutil.TempDir("", "memFlushTest")
	if err != nil {
		log.Fatal(err)
	}
	SSTDir = dir

	//setup partition
	partitionInfoMap[partitionId], _ = NewPartition(partitionId)
	DefaultConstants.memFlushWorker = 3
	DefaultConstants.compactWorker = 0
	activateMemFlushWorkers()
	deferFun := func() {
		os.Remove(ManifestFileName)
		manifestFile = nil
		os.RemoveAll(dir)
		fmt.Println("removed: \n- ", dir, "\n- ", ManifestFileName)
	}
	return deferFun
}

func TestMemFlushWriteSSTAndIndex_Overlap_Writes(t *testing.T) {
	partitionId := 0
	defer setupMemflushTest(partitionId)()

	//3 memtable with same recs with different timestamp
	iLog1 := partitionInfoMap[partitionId].prepareMemFlushInput(0, 900000, DefaultConstants.writeReq, 1)
	iLog2 := partitionInfoMap[partitionId].prepareMemFlushInput(0, 400000, DefaultConstants.writeReq, 2)
	iLog3 := partitionInfoMap[partitionId].prepareMemFlushInput(0, 500000, DefaultConstants.writeReq, 3)

	//add iLogs to pInfo
	partitionInfoMap[partitionId].inactiveLogDetails = []*InactiveLogDetails{&iLog1, &iLog2, &iLog3}

	//push it to memflush queue
	publishMemFlushTask(&iLog1)
	publishMemFlushTask(&iLog2)
	publishMemFlushTask(&iLog3)
	time.Sleep(5 * time.Second) //for worker to pickup task before calling stop
	stopMemFlushWorker()

	ts_3_count, ts_2_count, ts_1_count, others_count := 0, 0, 0, 0
	for _, value := range partitionInfoMap[partitionId].index.index {
		if value.TS == 3 {
			ts_3_count++
		} else if value.TS == 1 {
			ts_1_count++
		} else if value.TS == 2 {
			ts_2_count++
		} else {
			others_count++
		}
	}

	//validate index timestamps
	require.True(t, ts_3_count == 500000)
	require.True(t, ts_1_count == 400000)
	require.True(t, ts_2_count == 0)
	require.True(t, others_count == 0)

	//validate index rec count
	require.True(t, partitionInfoMap[partitionId].index.Size() == 900000)

	//validate inactive logs rec count
	require.True(t, len(partitionInfoMap[partitionId].inactiveLogDetails) == 0)

	//validate manifest
	replay()
	require.True(t, len(manifestFile.manifest.sstManifest[partitionId].manifestRecs) == 3)
	require.True(t, len(manifestFile.manifest.logManifest[partitionId].manifestRecs) == 3)
}

func TestMemFlushWriteSSTAndIndex_Overlap_Writes_Deletes(t *testing.T) {
	partitionId := 0
	defer setupMemflushTest(partitionId)()

	//3 memtable with same recs with different timestamp
	iLog1 := partitionInfoMap[partitionId].prepareMemFlushInput(0, 100000, DefaultConstants.writeReq, 1)
	iLog2 := partitionInfoMap[partitionId].prepareMemFlushInput(0, 100000, DefaultConstants.deleteReq, 2)
	iLog3 := partitionInfoMap[partitionId].prepareMemFlushInput(50000, 100000, DefaultConstants.writeReq, 3)

	//add iLogs to pInfo
	partitionInfoMap[partitionId].inactiveLogDetails = []*InactiveLogDetails{&iLog1, &iLog2, &iLog3}

	//push it to memflush queue
	publishMemFlushTask(&iLog1)
	publishMemFlushTask(&iLog2)
	publishMemFlushTask(&iLog3)
	time.Sleep(5 * time.Second) //for worker to pickup task before calling stop
	stopMemFlushWorker()

	ts_3_count, ts_2_count, ts_1_count, others_count := 0, 0, 0, 0
	for _, value := range partitionInfoMap[partitionId].index.index {
		if value.TS == 3 {
			ts_3_count++
		} else if value.TS == 1 {
			ts_1_count++
		} else if value.TS == 2 {
			ts_2_count++
		} else {
			others_count++
		}
	}

	//validate index timestamps
	require.True(t, ts_3_count == 50000)
	require.True(t, ts_1_count == 50000)
	require.True(t, ts_2_count == 0)
	require.True(t, others_count == 0)

	//validate index rec count -
	// memflush doesn't have effect on index for delete requests (gets removed while serving actual request)
	require.True(t, partitionInfoMap[partitionId].index.Size()== 100000)

	//validate inactive logs rec count
	require.True(t, len(partitionInfoMap[partitionId].inactiveLogDetails) == 0)

	//validate manifest
	replay()
	require.True(t, len(manifestFile.manifest.sstManifest[partitionId].manifestRecs) == 3)
	require.True(t, len(manifestFile.manifest.logManifest[partitionId].manifestRecs) == 3)
}

func TestMemFlushWriteSSTAndIndex_SSTMeta(t *testing.T) {
	partitionId := 0
	defer setupMemflushTest(partitionId)()

	iLog1 := partitionInfoMap[partitionId].prepareMemFlushInput(0, 100000, DefaultConstants.writeReq, 1)
	//mark one key as delete
	iLog1.MemTable.Insert([]byte("99Key_"), []byte(""), 1, DefaultConstants.deleteReq)

	seqNum := partitionInfoMap[partitionId].writeSSTAndIndex(iLog1.MemTable.Recs())

	//very results..
	require.True(t, 1 == len(partitionInfoMap[partitionId].levelsInfo[0].sstSeqNums))
	require.NotNil(t, partitionInfoMap[partitionId].levelsInfo[0].sstSeqNums[seqNum])
	require.True(t, partitionInfoMap[partitionId].sstReaderMap[seqNum].noOfWriteReq == 99999)
	require.True(t, partitionInfoMap[partitionId].sstReaderMap[seqNum].noOfDelReq == 1)
	require.True(t, string(partitionInfoMap[partitionId].sstReaderMap[seqNum].startKey) == "0Key_")
	require.True(t, string(partitionInfoMap[partitionId].sstReaderMap[seqNum].endKey) == "9Key_")
}

func (pInfo *PartitionInfo) prepareMemFlushInput(start int, end int, reqType byte, ts uint64) InactiveLogDetails {
	//Write data into mem and then flush it to sst
	memTable, _ := memstore.NewMemStore()

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
