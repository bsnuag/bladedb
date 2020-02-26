package bladedb

import (
	"bladedb/memstore"
	"bladedb/sklist"
	"fmt"
	"github.com/spaolacci/murmur3"
	"sync"
	"time"
)

var partitionInfoMap = make(map[int]*PartitionInfo)
var memFlushQueue = make(chan int, 100000)
var MemFlushQueueWG = &sync.WaitGroup{}

type PartitionInfo struct {
	readLock  sync.RWMutex //1. (WLock)Modifying MemTable during write  2. (RLock) Reading MemTable & Index
	writeLock sync.Mutex   //1. Modifying WAL & MemTable

	partitionId int

	logWriter *LogWriter

	sstWriter     *SSTWriter
	partitionMeta *PartitionMeta

	index              *sklist.SkipList //key(hash) - sstMetadata
	memTable           *memstore.MemTable
	inactiveLogDetails []*InactiveLogDetails //
	// should have which wal file it's a part of,
	// flush should happen without blocking others for long time

	levelLock    sync.RWMutex
	levelsInfo   map[int]*LevelInfo //TODO - check lock used while accessing levelsInfo - readlock is in use now..IMP
	sstReaderMap map[uint32]SSTReader

	compactLock      sync.Mutex
	activeCompaction *CompactInfo
}

type PartitionMeta struct {
	walSeq uint32
	sstSeq uint32
	lock   sync.Mutex
}

type MemTableFlushRec struct {
	PartitionId int
	LogFilename string
	WriteOffset int64
}

// - it fills partitionId (that belongs to native node/machine) and corresponding details in a PartitionDetailsRec record
//TODO - static info as of now
//TODO - persist this in file, so when reboot happens it gets info - we might not need this
//TODO - dynamically decides partition and ranges
//TODO - choose correct file name, sequence number etc for each writer i.e. wal, sst, index, etc

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
			partitionId:        partitionId,
			levelsInfo:         newLevelInfo(),
			index:              sklist.New(),
			memTable:           memTable,
			inactiveLogDetails: make([]*InactiveLogDetails, 0, 10), //expecting max of 10 inactive memtable
			sstReaderMap:       make(map[uint32]SSTReader),
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
		//pInfo.activatePeriodicWalFlush()
		partitionInfoMap[partitionId] = pInfo
	}
	fmt.Println("DB Setup Done")
	fmt.Println("--------------------------------")

	go activateMemFlush() //TODO - must support multiple threads

	//send signal to flush all inactive memtables
	for _, pInfo := range partitionInfoMap {
		pInfo.readLock.Lock()
		for i := 0; i < len(pInfo.inactiveLogDetails); i++ {
			fmt.Println("-- -- ", pInfo.inactiveLogDetails[i])
			memFlushQueue <- pInfo.partitionId
			MemFlushQueueWG.Add(1)
		}
		pInfo.readLock.Unlock()
	}
	for i := 0; i < defaultConstants.compactWorker; i++ {
		go runCompactWorker()
	}
	return nil
}

func newLevelInfo() map[int]*LevelInfo {
	levelsInfo := make(map[int]*LevelInfo)
	for lNo := 0; lNo <= defaultConstants.maxLevel; lNo++ {
		levelsInfo[lNo] = &LevelInfo{
			sstSeqNums: make(map[uint32]struct{}),
		}
	}
	return levelsInfo
}

//TODO - implement this post profiling
func (pInfo *PartitionInfo) activatePeriodicWalFlush() {
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
}

func Disp() {
	for _, v := range partitionInfoMap {
		fmt.Println(v)
	}
}

func PrintPartitionStats() {
	fmt.Println("---Printing Stats of Partitions---")

	for partId, pInfo := range partitionInfoMap {
		fmt.Println(fmt.Sprintf("Stats for partId: %d", partId))
		fmt.Println(fmt.Sprintf("No of Keys in Index: %d", pInfo.index.Length))
		fmt.Println(fmt.Sprintf("No of Keys in active Mem: %d", pInfo.memTable.Size()))
		var inactiveMemSize int64 = 0
		for _, inactiveLogDetail := range pInfo.inactiveLogDetails {
			inactiveMemSize += inactiveLogDetail.MemTable.Size()
		}
	}
	for partId, pInfo := range partitionInfoMap {
		fmt.Println("partitionId - ", partId)
		fmt.Println("inactive mems - ")
		for inactiveLogDetail := range pInfo.inactiveLogDetails {
			fmt.Println(inactiveLogDetail)
		}
		fmt.Println("active mems size - ", pInfo.memTable.Size())
		//fmt.Println("active mems - ", pInfo.memTable.Recs())

		fmt.Println("index size - ", pInfo.index.Length)

		//print index data

		//iterator := pInfo.index.NewIterator()

/*		for iterator.Next() {
			indexRec := iterator.Value().Value().(*IndexRec)
			fmt.Println(indexRec)
			iterator.Next()
		}
*/
		fmt.Println("levelsInfo size - ", len(pInfo.levelsInfo))
		for lno, levelInfo := range pInfo.levelsInfo { //TODO  - Do we need to put lock for levelsInfo //TODO - Check if go throws concurrent modification exception
			fmt.Println("level_num - ", lno)
			for sstSeqNum := range levelInfo.sstSeqNums {
				fmt.Println("sstreader - ", pInfo.sstReaderMap[sstSeqNum])
			}
		}
	}
}

func (pInfo *PartitionInfo) getNextSSTSeq() uint32 {
	meta := pInfo.partitionMeta

	meta.lock.Lock()
	defer meta.lock.Unlock()

	meta.sstSeq++
	return meta.sstSeq
}

func (pInfo *PartitionInfo) getNextLogSeq() uint32 {
	meta := pInfo.partitionMeta

	meta.lock.Lock()
	defer meta.lock.Unlock()

	meta.walSeq++
	return meta.walSeq
}

func GetNextLogSeq(partitionId int) uint32 {
	pInfo := partitionInfoMap[partitionId]
	if pInfo == nil {
		panic(fmt.Sprintf("Unable to find partitionInfo in partitionInfoMap for partition: %d", partitionId))
		return 0
	}

	return pInfo.getNextLogSeq()
}

//1. Stop Accepting all connections across all machines, stop all read/write - Pending - This is important - Pending - Do it via closing all connections from client
//2. Wait till all work is done by threads like walFlusher (implementation done), SSTMerger(no yet done) or any other
//3. Flush Wal and close - no new wal file - Done
//4. Once WAL is flushed, put all data into SST, add checkpoint in walstats file - Done
//5. Close log statWriter - Done
//TODO - check this again
//lock is not used assuming all requests will be blocked/stopped first
func Drain() {
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

		for _, levelInfo := range pInfo.levelsInfo { //TODO  - check lock for levels lock
			for sstSeqNum := range levelInfo.sstSeqNums {
				pInfo.sstReaderMap[sstSeqNum].Close()
			}
		}

		delete(partitionInfoMap, partitionId)
	}
}

func RestartAllOpr() {
	err := PreparePartitionIdsMap()

	if err != nil {

		panic(err)
	}
}

// this work similar to nodetool flush
//1. Flush WAL file, create another one
//2. send inactive_memtable_log info event to flush mem queue
func Flush() {
	waitGroup := &sync.WaitGroup{}
	waitGroup.Add(len(partitionInfoMap))//THIS is just to flush in parallel
	//
	for _, pInfo := range partitionInfoMap {
		go pInfo.flushPartition(waitGroup)
	}

	//waitGroup.Wait()
	fmt.Println(fmt.Sprintf("Flush and Rollover on request completed for all partitions, waiting for memflush to get complete.."))
	waitGroup.Wait()
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

func GetPartitionId(key interface{}) int {
	switch v := key.(type) {
	default:
		fmt.Printf("unexpected type %T", v)
		panic("Error while getting partitionId")
	case []byte:
		return int(murmur3.Sum64(key.([]byte)) % uint64(defaultConstants.noOfPartitions))
	case string:
		return int(murmur3.Sum64([]byte(key.(string))) % uint64(defaultConstants.noOfPartitions))
	}
}
