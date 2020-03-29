package bladedb

import (
	"bladedb/index"
	"bladedb/memstore"
	"fmt"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"sync"
	"sync/atomic"
)

var partitionInfoMap = make(map[int]*PartitionInfo)

type PartitionInfo struct {
	readLock  *sync.RWMutex //1. (WLock)Modifying MemTable during write  2. (RLock) Reading MemTable & Index
	writeLock *sync.Mutex   //1. Modifying WAL & MemTable

	partitionId int
	walSeq      uint32
	sstSeq      uint32

	logWriter *LogWriter

	index              *index.SkipList //key(hash) - sstMetadata
	memTable           *memstore.MemTable
	inactiveLogDetails []*InactiveLogDetails //
	// should have which wal file it's a part of,
	// flush should happen without blocking others for long time

	levelLock    *sync.RWMutex
	levelsInfo   map[int]*LevelInfo
	sstReaderMap map[uint32]SSTReader

	compactLock      *sync.Mutex
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

	for partitionId := 0; partitionId < DefaultConstants.noOfPartitions; partitionId++ {

		pInfo, err := NewPartition(partitionId)
		if err != nil {
			errors.Wrapf(err, "Error while creating new partition: %d", partitionId)
		}

		partitionInfoMap[partitionId] = pInfo

		maxSSTSeq, err := pInfo.loadActiveSSTs()
		if err != nil {
			fmt.Println(err)
			return err
		}

		maxLogSeq, err := maxLogSeq(partitionId)
		if err != nil {
			fmt.Println(err)
			return err
		}

		pInfo.sstSeq = maxSSTSeq
		pInfo.walSeq = maxLogSeq

		nextLogSeq := pInfo.getNextLogSeq()
		logWriter, err := newLogWriter(partitionId, nextLogSeq)

		if err != nil {
			fmt.Println(err)
			return err
		}

		pInfo.logWriter = logWriter
		pInfo.loadUnclosedLogFile()
	}

	loadDbStateGroup := errgroup.Group{}
	for partitionId := 0; partitionId < DefaultConstants.noOfPartitions; partitionId++ {
		pInfo := partitionInfoMap[partitionId]
		loadDbStateGroup.Go(func() error {
			if err := pInfo.loadUnclosedLogFile(); err != nil {
				return errors.Wrapf(err, "Error while loading unclosed log files from log-manifest: %v",
					manifestFile.manifest.logManifest[pInfo.partitionId])
			}
			for _, reader := range pInfo.sstReaderMap {
				if _, err := (&reader).loadSSTRec(pInfo.index); err != nil {
					return errors.Wrapf(err, "Error while loading index from active ssts : %v", pInfo.sstReaderMap)
				}
			}
			return nil
		})
	}
	if err := loadDbStateGroup.Wait(); err != nil {
		return errors.Wrap(err, "Error while setting up db")
	}

	fmt.Println("DB Setup Done")

	activateMemFlushWorkers()
	activateCompactWorkers()
	flushPendingMemTables()
	return nil
}

func NewPartition(partitionId int) (*PartitionInfo, error) {
	memTable, err := memstore.NewMemStore(partitionId)
	if err != nil {
		return nil, err
	}
	return &PartitionInfo{
		readLock:           &sync.RWMutex{},
		writeLock:          &sync.Mutex{},
		partitionId:        partitionId,
		levelsInfo:         newLevelInfo(),
		index:              index.NewIndex(),
		memTable:           memTable,
		inactiveLogDetails: make([]*InactiveLogDetails, 0, 10), //expecting max of 10 inactive memtable
		sstReaderMap:       make(map[uint32]SSTReader),
		levelLock:          &sync.RWMutex{},
		compactLock:        &sync.Mutex{},
		sstSeq:             0,
		walSeq:             0,
	}, nil
}
func flushPendingMemTables() {
	for _, pInfo := range partitionInfoMap {
		for i := 0; i < len(pInfo.inactiveLogDetails); i++ {
			fmt.Println("-- -- ", pInfo.inactiveLogDetails[i])
			publishMemFlushTask(pInfo.inactiveLogDetails[i])
		}
	}
}

//TODO - implement this post profiling if required
/*func (pInfo *PartitionInfo) activatePeriodicWalFlush() {
	ticker := time.NewTicker(DefaultConstants.walFlushPeriodInSec)
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
	return atomic.AddUint32(&pInfo.sstSeq, 1)
}

func (pInfo *PartitionInfo) getNextLogSeq() uint32 {
	return atomic.AddUint32(&pInfo.walSeq, 1)
}

func closeAllActiveSSTReaders() {
	for partitionId, pInfo := range partitionInfoMap {
		for _, sstReader := range pInfo.sstReaderMap {
			sstReader.Close()
		}
		delete(partitionInfoMap, partitionId) //clear partition map
	}
}

//Post Drain no db operation is carried out. Need to restart db to begin db operations
//1. Stop Accepting all connections across all machines, stop all read/write - Pending - This is important - Pending - Do it via closing all connections from client
//2. Wait till all work is done by threads like walFlusher (implementation done), SSTMerger(no yet done) or any other
//3. Flush Wal and close - no new wal file - Done
//4. Once WAL is flushed, put all data into SST, add checkpoint in walstats file - Done

func Drain() {
	fmt.Println("drain called")
	//1. TODO - Stop all accepting incoming db requests
	for partitionId, pInfo := range partitionInfoMap {
		fmt.Println(fmt.Sprintf("Closing all operation for partId: %d", partitionId))
		//3rd point
		rolledOverLogDetails, logFlushErr := pInfo.logWriter.FlushAndClose()
		if logFlushErr != nil {
			panic(logFlushErr)
		}
		//4th point
		if rolledOverLogDetails != nil && rolledOverLogDetails.WriteOffset > 0 {
			pInfo.handleRolledOverLogDetails(rolledOverLogDetails)
		}
	}
	stopMemFlushWorker()
	stopCompactWorker()
	closeAllActiveSSTReaders()
	closeManifest()
}

// works similar to nodetool flush
//1. Flush WAL file, create another one
//2. send inactive_memtable_log info event to flush mem queue
func Flush() {
	waitGroup := &sync.WaitGroup{}

	waitGroup.Add(len(partitionInfoMap)) //THIS is just to flush in parallel
	for _, pInfo := range partitionInfoMap {
		go pInfo.flushPartition(waitGroup)
	}
	waitGroup.Wait()

	fmt.Println(fmt.Sprintf("Flush and Rollover on request completed for all partitions"))
}

func (pInfo *PartitionInfo) flushPartition(wg *sync.WaitGroup) {
	pInfo.writeLock.Lock()
	fmt.Println(fmt.Sprintf("Flush and Rollover on request, partId: %d", pInfo.partitionId))
	rolledOverLogDetails, err := pInfo.logWriter.FlushAndRollOver()
	if err != nil {
		panic(err)
	}
	fmt.Println("rolledOverLogDetails1- ", rolledOverLogDetails)
	if rolledOverLogDetails != nil && rolledOverLogDetails.WriteOffset > 0 {
		pInfo.handleRolledOverLogDetails(rolledOverLogDetails)
	}
	pInfo.writeLock.Unlock()
	wg.Done()
}

