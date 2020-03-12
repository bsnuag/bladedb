package bladedb

import (
	"bladedb/memstore"
	"bladedb/sklist"
	"fmt"
	"sync"
	"time"
)

var memFlushTaskQueue = make(chan *InactiveLogDetails, 100000)
var activeMemFlushSubscriber sync.WaitGroup

//activate mem-flush and compact worker
func activateMemFlushWorkers() {
	for i := 1; i <= defaultConstants.memFlushWorker; i++ {
		go memFlushWorker(fmt.Sprintf("MemFlushWorker- %d", i)) //TODO - must support multiple threads..V.V Imp Next
	}
}

func memFlushWorker(flushWorkerName string) {
	activeMemFlushSubscriber.Add(1)
	defer activeMemFlushSubscriber.Done()
	for {
		memTask, ok := <-memFlushTaskQueue
		if !ok {
			fmt.Println(fmt.Sprintf("Received signal to stop mem flush worker, exiting: %s", flushWorkerName))
			break
		}
		start := time.Now()
		pInfo := partitionInfoMap[memTask.PartitionId]
		if pInfo == nil {
			panic(fmt.Sprintf("Could not find partition Info for partition number %d", memTask.PartitionId))
		}
		recs := memTask.MemTable.Recs()
		fmt.Println(fmt.Sprintf("replaced mem-table for partitionId: %d, old-mem-table size: %d", memTask.PartitionId, recs.Length))
		seqNum := pInfo.writeSSTAndIndex(recs)

		//sst and inactiveLogDetails's file is closed safely
		mf1 := ManifestRec{
			partitionId: memTask.PartitionId,
			seqNum:      memTask.FileSeqNum,
			fop:         defaultConstants.fileDelete,
			fileType:    defaultConstants.logFileType,
		}

		mf2 := ManifestRec{
			partitionId: memTask.PartitionId,
			levelNum:    0,
			seqNum:      seqNum,
			fop:         defaultConstants.fileCreate,
			fileType:    defaultConstants.sstFileType,
		}
		//writeManifest log-delete and sst-create details
		writeManifest([]ManifestRec{mf1, mf2})

		newInactiveLogs := make([]*InactiveLogDetails, 0)
		//remove flushed log details
		pInfo.readLock.Lock()
		for _, ele := range pInfo.inactiveLogDetails {
			if ele.FileSeqNum != memTask.FileSeqNum && ele.PartitionId != memTask.PartitionId {
				newInactiveLogs = append(newInactiveLogs, ele)
			}
		}
		pInfo.inactiveLogDetails = newInactiveLogs
		pInfo.readLock.Unlock()
		deleteLog(memTask.PartitionId, memTask.FileSeqNum)
		//check for possible compaction
		pInfo.level0PossibleCompaction()
		fmt.Println(fmt.Sprintf("Total time to flush mem table: %f, total Bytes: %d, partId: %d",
			time.Since(start).Seconds(), memTask.WriteOffset, memTask.PartitionId))
	}
}

//Stop reading while flushing data to sst, sst recs might not be available before it exits in indexRec
func (pInfo *PartitionInfo) writeSSTAndIndex(memRecs *sklist.SkipList) (seqNum uint32) {
	//sstReader meta
	var startKey []byte = nil
	var endKey []byte = nil
	var noOfDelReq uint64 = 0
	var noOfWriteReq uint64 = 0
	sstWriter, err := pInfo.NewSSTWriter()
	if err != nil {
		panic(err)
	}
	idxmap := make(map[string]interface{})
	//flush memRecs to SST and index
	iterator := memRecs.NewIterator()
	for iterator.Next() {
		next := iterator.Value()
		value := next.Value().(*memstore.MemRec)
		indexRec := &IndexRec{
			SSTRecOffset:  sstWriter.Offset,
			SSTFileSeqNum: sstWriter.SeqNum,
			TS:            value.TS,
		}
		_, sstErr := sstWriter.Write(value.Key, value.Value, value.TS, value.RecType)
		if sstErr != nil {
			panic(sstErr)
		}

		//if rec type is writeReq then load to index, delete request need not load to index
		if value.RecType == defaultConstants.writeReq {
			keyHash, _ := GetHash(value.Key)
			idxmap[keyHash] = indexRec
			noOfWriteReq++
		} else {
			noOfDelReq++
		}

		if startKey == nil {
			startKey = value.Key
		}
		endKey = value.Key
	}

	flushedFileSeqNum, err := sstWriter.FlushAndClose()

	if err != nil {
		fmt.Println("Error while flushing data sst")
		panic(err)
	}
	//update index
	pInfo.index.SetBatch(idxmap)

	levelNum := 0
	sstReader, err := NewSSTReader(flushedFileSeqNum, pInfo.partitionId)
	if err != nil {
		fmt.Println(fmt.Sprintf("Failed to update reader map for sstFileSeqNum: %d", flushedFileSeqNum))
		panic(err)
	}

	sstReader.startKey = startKey
	sstReader.endKey = endKey
	sstReader.noOfWriteReq = noOfWriteReq
	sstReader.noOfDelReq = noOfDelReq

	//update sstReader map
	pInfo.levelLock.Lock()
	defer pInfo.levelLock.Unlock()

	pInfo.levelsInfo[levelNum].sstSeqNums[flushedFileSeqNum] = struct{}{}
	pInfo.sstReaderMap[flushedFileSeqNum] = sstReader

	return sstWriter.SeqNum
}

func publishMemFlushTask(inactiveLogDetails *InactiveLogDetails) {
	memFlushTaskQueue <- inactiveLogDetails
}

func stopMemFlushWorker() {
	fmt.Println("Request received to stop MemFlush workers", memFlushTaskQueue)
	close(memFlushTaskQueue)
	fmt.Println("Waiting for all submitted mem flush tasks to be completed")
	activeMemFlushSubscriber.Wait()
	fmt.Println("all submitted MemFlush tasks completed")
}
