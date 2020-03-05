package bladedb

import (
	"fmt"
	"sync"
)

func Open() error {
	err := createDirectory(LogDir)
	if err != nil {
		fmt.Println(fmt.Sprintf("Error while creating directory: %s, error: %v", LogDir, err))
		return err
	}

	err = createDirectory(SSTDir)
	if err != nil {
		fmt.Println(fmt.Sprintf("Error while creating directory: %s, error: %v", SSTDir, err))
	}

	err = PreparePartitionIdsMap()
	if err != nil {
		panic(err)
	}
	return err
}

func Close() {
	Drain()
}

//Post Drain no db operation is carried out. Need to restart db to begin db operations
//1. Stop Accepting all connections across all machines, stop all read/write - Pending - This is important - Pending - Do it via closing all connections from client
//2. Wait till all work is done by threads like walFlusher (implementation done), SSTMerger(no yet done) or any other
//3. Flush Wal and close - no new wal file - Done
//4. Once WAL is flushed, put all data into SST, add checkpoint in walstats file - Done

func Drain() {
	//1. TODO - Stop all accepting incoming db requests
	for partitionId, pInfo := range partitionInfoMap {
		fmt.Println(fmt.Sprintf("Closing all operation for partId: %d", partitionId))
		pInfo.writeLock.Lock() //TODO - V.V. Imp - This is just to assume no write requests are pending when we call drain, once network part is ready replace this with point 1.
		//3rd point
		rolledOverLogDetails, logFlushErr := pInfo.logWriter.FlushAndClose()
		if logFlushErr != nil {
			panic(logFlushErr)
		}
		//4th point
		if rolledOverLogDetails != nil && rolledOverLogDetails.WriteOffset > 0 {
			pInfo.handleRolledOverLogDetails(rolledOverLogDetails)
		}
		pInfo.writeLock.Unlock()
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

func PrintPartitionStats() {
	fmt.Println("------------------------------------X Printing DB Stats X------------------------------------")

	for partId, pInfo := range partitionInfoMap {
		fmt.Println(fmt.Sprintf("\n\n--------------- Stats for PartitionId: %d ---------------", partId))
		fmt.Println(fmt.Sprintf("PartitionId: %d", partId))
		fmt.Println(fmt.Sprintf("No of Keys in Index: %d", pInfo.index.Length))
		fmt.Println(fmt.Sprintf("No of Keys in active Mem: %d", pInfo.memTable.Size()))
		var inactiveMemSize int64 = 0
		for _, inactiveLogDetail := range pInfo.inactiveLogDetails {
			inactiveMemSize += inactiveLogDetail.MemTable.Size()
		}
		fmt.Println(fmt.Sprintf("No of Keys in inactive Mem: %d", inactiveMemSize))
		for l, lInfo := range pInfo.levelsInfo {
			fmt.Println(fmt.Sprintf("In Level: %d, Number of SSTable: %d", l, len(lInfo.sstSeqNums)))
			fmt.Println(fmt.Sprintf("In Level: %d, SSTable SeqNums: %v", l, lInfo.sstSeqNums))
		}
		var totalWrite uint64 = 0
		var totalDelete uint64 = 0
		for num, reader := range pInfo.sstReaderMap {
			fmt.Println(fmt.Sprintf("SST SeqNum: %d, Total Write: %d, Total Delete: %d", num,
				reader.noOfWriteReq, reader.noOfDelReq))
			totalWrite += reader.noOfWriteReq
			totalDelete += reader.noOfDelReq
		}
		fmt.Println(fmt.Sprintf("PartId: %d, Total Write: %d, Total Delete: %d", partId,
			totalWrite, totalDelete))
		fmt.Println()
		fmt.Println()
	}
}
