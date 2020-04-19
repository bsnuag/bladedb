package bladedb

import (
	"fmt"
	"sync/atomic"
	"time"
)

func activateCompactWorkers() {
	if db.config.CompactWorker != 0 {
		db.compactTaskQueue = make(chan *CompactInfo, 10000)
		db.compactActive = 1
	}
	for i := 1; i <= db.config.CompactWorker; i++ {
		go compactWorker(fmt.Sprintf("CompactWorker- %d", i))
	}
}

//blocks from accepting new compact tasks, but waits to complete already submitted tasks
func stopCompactWorker() {
	if db.config.CompactWorker == 0 {
		return
	}
	startT := time.Now()
	db.logger.Info().Msg("Request received to stop compact workers, " +
		"No new compaction task would be taken, published tasks will be completed")

	atomic.CompareAndSwapInt32(&db.compactActive, 1, 0)
	close(db.compactTaskQueue)
	db.logger.Info().Msg("Waiting for all submitted compaction tasks to complete")
	db.compactSubscriber.Wait()

	db.logger.Info().
		Float64("Duration (Seconds)", time.Since(startT).Seconds()).
		Msg("all submitted compaction tasks completed")
}

//publish new task to compact queue if compaction is active(db.compactActive==true)
func publishCompactTask(compactTask *CompactInfo) {
	if isCompactionActive() {
		db.compactTaskQueue <- compactTask
	} else {
		db.logger.Info().Msg("Compaction is not active, cannot publish new task")
	}
}

func isCompactionActive() bool {
	return atomic.LoadInt32(&db.compactActive) == 1
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
	pInfo.readLock.Lock()
	compactInfo := pInfo.activeCompaction
	for _, newReader := range compactInfo.newSSTReaders {
		mf1 := ManifestRec{
			partitionId: pInfo.partitionId,
			seqNum:      newReader.SeqNm,
			levelNum:    compactInfo.nextLevel,
			fop:         fCreate,
			fileType:    DataFileType,
		}
		manifestRecs = append(manifestRecs, mf1)
		pInfo.sstReaderMap[newReader.SeqNm] = newReader
		pInfo.levelsInfo[compactInfo.nextLevel].sstSeqNums[newReader.SeqNm] = struct{}{}
	}
	pInfo.readLock.Unlock()
	return manifestRecs
}
