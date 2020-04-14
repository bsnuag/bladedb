package bladedb

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var compactTaskQueue chan *CompactInfo = nil //TODO - change channel type to compactTask with pId and thisLevel
var compactSubscriber sync.WaitGroup
var compactActive int32 = 0 //0- false, 1 - true

func activateCompactWorkers() {
	if DefaultConstants.compactWorker != 0 {
		compactTaskQueue = make(chan *CompactInfo, 10000)
		compactActive = 1
	}
	for i := 1; i <= DefaultConstants.compactWorker; i++ {
		go compactWorker(fmt.Sprintf("CompactWorker- %d", i))
	}
}

//blocks from accepting new compact tasks, but waits to complete already submitted tasks
func stopCompactWorker() {
	if DefaultConstants.compactWorker == 0 {
		return
	}
	startT := time.Now()
	defaultLogger.Info().Msg("Request received to stop compact workers, " +
		"No new compaction task would be taken, published tasks will be completed")

	atomic.CompareAndSwapInt32(&compactActive, 1, 0)
	close(compactTaskQueue)
	defaultLogger.Info().Msg("Waiting for all submitted compaction tasks to complete")
	compactSubscriber.Wait()

	defaultLogger.Info().
		Float64("Duration (Seconds)", time.Since(startT).Seconds()).
		Msg("all submitted compaction tasks completed")
}

//publish new task to compact queue if compaction is active(compactActive==true)
func publishCompactTask(compactTask *CompactInfo) {
	if isCompactionActive() {
		compactTaskQueue <- compactTask
	} else {
		defaultLogger.Info().Msg("Compaction is not active, cannot publish new task")
	}
}

func isCompactionActive() bool {
	return atomic.LoadInt32(&compactActive) == 1
}

func (compactInfo *CompactInfo) compactFileNames() (botLevelSSTNames []string, topLevelSSTNames []string, newSSTNames []string) {
	for _, reader := range compactInfo.botLevelSST {
		botLevelSSTNames = append(botLevelSSTNames, reader.file.Name())
	}
	for _, reader := range compactInfo.topLevelSST {
		topLevelSSTNames = append(topLevelSSTNames, reader.file.Name())
	}
	for _, reader := range compactInfo.newSSTReaders {
		newSSTNames = append(newSSTNames, reader.file.Name())
	}
	return botLevelSSTNames, topLevelSSTNames, newSSTNames
}

func (pInfo PartitionInfo) activateNewSSTs() (manifestRecs []ManifestRec) {
	compactInfo := pInfo.activeCompaction
	for _, newReader := range compactInfo.newSSTReaders {
		mf1 := ManifestRec{
			partitionId: pInfo.partitionId,
			seqNum:      newReader.SeqNm,
			levelNum:    compactInfo.nextLevel,
			fop:         DefaultConstants.fileCreate,
			fileType:    DefaultConstants.sstFileType,
		}
		manifestRecs = append(manifestRecs, mf1)
		pInfo.sstReaderMap[newReader.SeqNm] = newReader
		pInfo.levelsInfo[compactInfo.nextLevel].sstSeqNums[newReader.SeqNm] = struct{}{}
	}
	return manifestRecs
}
