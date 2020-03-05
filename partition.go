package bladedb

import (
	"bladedb/memstore"
	"bladedb/sklist"
	"fmt"
	"sync"
)

var partitionInfoMap = make(map[int]*PartitionInfo)

type PartitionInfo struct {
	readLock  sync.RWMutex //1. (WLock)Modifying MemTable during write  2. (RLock) Reading MemTable & Index
	writeLock sync.Mutex   //1. Modifying WAL & MemTable

	partitionId int

	logWriter *LogWriter

	partitionMeta *PartitionMeta

	index              *sklist.SkipList //key(hash) - sstMetadata
	memTable           *memstore.MemTable
	inactiveLogDetails []*InactiveLogDetails //
	// should have which wal file it's a part of,
	// flush should happen without blocking others for long time

	levelLock    sync.RWMutex
	levelsInfo   map[int]*LevelInfo //TODO - check lock used while accessing levelsInfo..do we need it? - readlock is in use now..IMP
	sstReaderMap map[uint32]SSTReader

	compactLock      sync.Mutex
	activeCompaction *CompactInfo
}

type PartitionMeta struct {
	walSeq uint32
	sstSeq uint32
	lock   sync.Mutex
}

//TODO - dynamically decides partition and ranges

// - it fills partitionId (that belongs to native node/machine) and corresponding details in a PartitionDetailsRec record
func PreparePartitionIdsMap() error {

	fmt.Println("Setting up DB")
	initManifest()

	for partitionId := 0; partitionId < defaultConstants.noOfPartitions; partitionId++ {

		memTable, err := memstore.NewMemStore(partitionId)
		if err != nil {
			fmt.Println(err)
			return err
		}

		pInfo := &PartitionInfo{
			readLock:           sync.RWMutex{},
			writeLock:          sync.Mutex{},
			partitionId:        partitionId,
			levelsInfo:         newLevelInfo(),
			index:              sklist.New(),
			memTable:           memTable,
			inactiveLogDetails: make([]*InactiveLogDetails, 0, 10), //expecting max of 10 inactive memtable
			sstReaderMap:       make(map[uint32]SSTReader),
			levelLock:          sync.RWMutex{},
			compactLock:        sync.Mutex{},
		}

		maxSSTSeq, err := loadPartitionData(partitionId)
		if err != nil {
			fmt.Println(err)
			return err
		}

		maxLogSeq, err := maxLogSeq(partitionId)
		if err != nil {
			fmt.Println(err)
			return err
		}

		pInfo.partitionMeta = &PartitionMeta{
			sstSeq: maxSSTSeq,
			walSeq: maxLogSeq,
		}

		nextLogSeq := pInfo.getNextLogSeq()
		logWriter, err := newLogWriter(partitionId, nextLogSeq)

		if err != nil {
			fmt.Println(err)
			return err
		}

		pInfo.logWriter = logWriter

		pInfo.loadUnclosedLogFile()
		partitionInfoMap[partitionId] = pInfo
	}
	fmt.Println("DB Setup Done")

	activateMemFlushWorkers()
	activateCompactWorkers()
	flushPendingMemTables()
	return nil
}

func flushPendingMemTables() {
	for _, pInfo := range partitionInfoMap {
		pInfo.readLock.Lock()
		for i := 0; i < len(pInfo.inactiveLogDetails); i++ {
			fmt.Println("-- -- ", pInfo.inactiveLogDetails[i])
			publishMemFlushTask(pInfo.inactiveLogDetails[i])
		}
		pInfo.readLock.Unlock()
	}
}

//TODO - implement this post profiling if required
/*func (pInfo *PartitionInfo) activatePeriodicWalFlush() {
	ticker := time.NewTicker(defaultConstants.walFlushPeriodInSec)
	go func() {
		for {
			select {
			case <-ticker.C:
				pInfo.writeLock.Lock()
				fmt.Println("Ticked to flush wal file for partId: ", pInfo.partitionId)
				if err := pInfo.logWriter.FlushAndSync(); err != nil {
					fmt.Println("Error while flushing wal file for partId: ", pInfo.partitionId)
					panic(err)
				}
				pInfo.writeLock.Unlock()
				//case <-pInfo.walFlushQueue:
				//	fmt.Println("Received Req to stop periodic WAL flush for partId: ", pInfo.partitionId)
				//	ticker.Stop()
				//	return
			}
		}
	}()
}*/

func (pInfo *PartitionInfo) getNextSSTSeq() uint32 {
	meta := pInfo.partitionMeta

	meta.lock.Lock()
	defer meta.lock.Unlock()

	meta.sstSeq++ //this can be replaced with atomic counter
	return meta.sstSeq
}

func (pInfo *PartitionInfo) getNextLogSeq() uint32 {
	meta := pInfo.partitionMeta

	meta.lock.Lock()
	defer meta.lock.Unlock()

	meta.walSeq++
	return meta.walSeq
}

func closeAllActiveSSTReaders() {
	for partitionId, pInfo := range partitionInfoMap {
		for _, sstReader := range pInfo.sstReaderMap {
			sstReader.Close()
		}
		delete(partitionInfoMap, partitionId) //clear partition map
	}
}
