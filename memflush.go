package bladedb

import (
	"bladedb/memstore"
	"bladedb/sklist"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var memFlushTaskQueue chan *InactiveLogDetails = nil
var activeMemFlushSubscriber sync.WaitGroup
var memFlushActive int32 = 0

//activate mem-flush and compact worker
func activateMemFlushWorkers() {
	if DefaultConstants.memFlushWorker != 0 {
		memFlushTaskQueue = make(chan *InactiveLogDetails, 100000)
		memFlushActive = 1
	}

	for i := 1; i <= DefaultConstants.memFlushWorker; i++ {
		go memFlushWorker(fmt.Sprintf("MemFlushWorker- %d", i))
	}
}

func memFlushWorker(flushWorkerName string) {
	activeMemFlushSubscriber.Add(1)
	defer activeMemFlushSubscriber.Done()
	for {
		memTask, ok := <-memFlushTaskQueue
		if !ok {
			defaultLogger.Info().Msgf("Received signal to stop mem flush worker, exiting: %s", flushWorkerName)
			break
		}
		start := time.Now()
		pInfo := partitionInfoMap[memTask.PartitionId]
		if pInfo == nil {
			defaultLogger.Fatal().Msgf("Could not find partition for partition number %d", memTask.PartitionId)
		}

		recs := memTask.MemTable.Recs()
		seqNum, err := pInfo.writeSSTAndIndex(recs)
		//this memtable will still be in memory, but won't be attempted for re-flush. Should this be retried for n times ?
		if err != nil {
			defaultLogger.Err(err).Int("partitionId", memTask.PartitionId).
				Msg("Error while flushing memtable, aborting memflush")
			continue
		}

		//sst and inactiveLogDetails's file is closed safely
		mf1 := ManifestRec{
			partitionId: memTask.PartitionId,
			seqNum:      memTask.FileSeqNum,
			fop:         DefaultConstants.fileDelete,
			fileType:    DefaultConstants.logFileType,
		}

		mf2 := ManifestRec{
			partitionId: memTask.PartitionId,
			levelNum:    0,
			seqNum:      seqNum,
			fop:         DefaultConstants.fileCreate,
			fileType:    DefaultConstants.sstFileType,
		}
		//writeManifest log-delete and sst-create details
		writeManifest([]ManifestRec{mf1, mf2})

		newInactiveLogs := make([]*InactiveLogDetails, 0, len(pInfo.inactiveLogDetails))
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
		pInfo.level0PossibleCompaction()

		defaultLogger.Info().
			Int("partition", memTask.PartitionId).
			Uint32("bytes flushed", memTask.WriteOffset).
			Float64("duration (seconds)", time.Since(start).Seconds()).
			Msg("MemFlush Completed")
	}
}

//Stop reading while flushing data to sst, sst recs might not be available before it exits in indexRec
func (pInfo *PartitionInfo) writeSSTAndIndex(memRecs *sklist.SkipList) (uint32, error) {
	var sstEncoderBuf = make([]byte, sstBufLen)
	var startKey, endKey []byte = nil, nil
	var noOfDelReq, noOfWriteReq, indexOffset uint32 = 0, 0, 0
	sstWriter, err := pInfo.NewSSTWriter()
	if err != nil {
		return 0, err
	}
	//flush memRecs to SST and index
	iterator := memRecs.NewIterator()
	for iterator.Next() {
		next := iterator.Value()
		key := []byte(next.Key())
		value := next.Value().(*memstore.MemRec)
		sstRec := SSTRec{value.RecType, key, value.Value, value.TS}
		n := sstRec.SSTEncoder(sstEncoderBuf[:])
		nn, sstErr := sstWriter.Write(sstEncoderBuf[:n])
		if sstErr != nil {
			return 0, err
		}

		//if rec type is writeReq then load to index, delete request need not load to index
		if value.RecType == DefaultConstants.writeReq {
			indexRec := IndexRec{
				SSTRecOffset:  indexOffset,
				SSTFileSeqNum: sstWriter.SeqNum,
				TS:            value.TS,
			}
			keyHash := Hash(key[:])
			pInfo.index.Set(keyHash, indexRec)
			noOfWriteReq++
		} else {
			noOfDelReq++
		}

		if startKey == nil {
			startKey = key
		}
		endKey = key
		indexOffset += nn
	}

	if err := sstWriter.FlushAndClose(); err != nil {
		return 0, err
	}

	levelNum := 0
	sstReader, err := NewSSTReader(sstWriter.SeqNum, pInfo.partitionId)
	if err != EmptyFile && err != nil {
		return 0, err
	}

	sstReader.startKey = startKey
	sstReader.endKey = endKey
	sstReader.noOfWriteReq = noOfWriteReq
	sstReader.noOfDelReq = noOfDelReq

	//update sstReader map
	pInfo.levelLock.Lock()
	defer pInfo.levelLock.Unlock()

	pInfo.levelsInfo[levelNum].sstSeqNums[sstWriter.SeqNum] = struct{}{}
	pInfo.sstReaderMap[sstWriter.SeqNum] = sstReader

	return sstWriter.SeqNum, nil
}

func publishMemFlushTask(inactiveLogDetails *InactiveLogDetails) {
	if isMemFlushActive() {
		memFlushTaskQueue <- inactiveLogDetails
	} else {
		defaultLogger.Info().Msg("MemFlush is not active, cannot publish new task")
	}
}

func isMemFlushActive() bool {
	return memFlushActive == 1 //TODO - will there be race around it since read is not atomic?
}

func stopMemFlushWorker() {
	if DefaultConstants.memFlushWorker == 0 {
		return
	}
	defaultLogger.Info().Msg("Request received to stop MemFlush workers")

	atomic.AddInt32(&memFlushActive, -1)
	close(memFlushTaskQueue)

	defaultLogger.Info().Msg("Waiting for all submitted mem flush tasks to complete")
	activeMemFlushSubscriber.Wait()
	defaultLogger.Info().Msg("all submitted MemFlush tasks completed")
}
