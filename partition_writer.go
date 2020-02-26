package bladedb

import (
	"bladedb/memstore"
	"bladedb/sklist"
	"fmt"
)

func Remove(key string, ts int64) {
	keyByte := []byte(key)
	keyHash, _ := GetHash(keyByte)
	partitionId := GetPartitionId(keyHash)
	pInfo := partitionInfoMap[partitionId]

	if pInfo == nil {
		panic(fmt.Sprintf("PartitionInfo doesn't exists for partition: %d, Key: %s ", partitionId, key))
	}

	pInfo.writeLock.Lock()
	inactiveLogDetails, err := pInfo.logWriter.Write(keyByte, nil, ts, defaultConstants.deleteReq)

	if err != nil {
		panic(err)
	}

	if inactiveLogDetails != nil && inactiveLogDetails.WriteOffset > 0 {
		pInfo.handleRolledOverLogDetails(inactiveLogDetails)
	}

	pInfo.readLock.Lock()
	pInfo.memTable.Insert(keyByte, nil, ts, defaultConstants.deleteReq) //We need to put delete rec in mem, since it gets into SST later
	pInfo.index.Remove(keyHash)
	pInfo.readLock.Unlock()

	pInfo.writeLock.Unlock()

}

func Put(key string, value string, ts int64) {
	keyByte := []byte(key)
	valueByte := []byte(value)
	keyHash, _ := GetHash(keyByte)
	partitionId := GetPartitionId(keyHash)
	pInfo := partitionInfoMap[partitionId]

	if pInfo == nil {
		panic(fmt.Sprintf("PartitionInfo doesn't exists for partition: %d, Key: %s ", partitionId, key))
	}

	//fmt.Println(fmt.Sprintf("Write Req for Key: %s, partId: %d", key, partitionId))
	pInfo.writeLock.Lock()
	inactiveLogDetails, err := pInfo.logWriter.Write(keyByte, valueByte, ts, defaultConstants.writeReq)

	if err != nil {
		panic(err)
	}

	if inactiveLogDetails != nil && inactiveLogDetails.WriteOffset > 0 {
		pInfo.handleRolledOverLogDetails(inactiveLogDetails)
	}

	pInfo.readLock.Lock()
	pInfo.memTable.Insert(keyByte, valueByte, ts, defaultConstants.writeReq)
	pInfo.readLock.Unlock()

	pInfo.writeLock.Unlock()
}

func (pInfo *PartitionInfo) handleRolledOverLogDetails(inactiveLogDetails *InactiveLogDetails) {
	pInfo.readLock.Lock()
	defer pInfo.readLock.Unlock()

	fmt.Println("inactiveLogDetails: ", inactiveLogDetails)

	oldMemTable := pInfo.memTable

	memTable, err := memstore.NewMemStore(pInfo.partitionId)
	if err != nil {
		panic(err)
		return
	}
	pInfo.memTable = memTable //update active memtable to a new one

	inactiveLogDetails.MemTable = oldMemTable
	pInfo.inactiveLogDetails = append(pInfo.inactiveLogDetails, inactiveLogDetails)
	memFlushQueue <- pInfo.partitionId
	MemFlushQueueWG.Add(1)
}

func activateMemFlush() {
	for pNum := range memFlushQueue {
		pInfo := partitionInfoMap[pNum]
		fmt.Println("FlushMemTableReq partNo ", pNum)
		if pInfo == nil {
			panic(fmt.Sprintf("Could not find partition Info for partition number %d", pNum))
		}

		//take a copy of first
		var inactiveLogDetails *InactiveLogDetails = nil
		pInfo.readLock.RLock()
		if len(pInfo.inactiveLogDetails) == 0 {
			//throw error
		}
		inactiveLogDetails = pInfo.inactiveLogDetails[0]
		fmt.Println(inactiveLogDetails)
		pInfo.readLock.RUnlock()

		println(inactiveLogDetails)
		pInfo.sstWriter, _ = pInfo.NewSSTWriter()
		recs := inactiveLogDetails.MemTable.Recs()
		fmt.Println(fmt.Sprintf("replaced mem-table for partitionId: %d, old-mem-table size: %d", pNum, recs.Length))

		pInfo.writeSSTAndIndex(recs)

		//sst and inactiveLogDetails's file is closed safely
		mf1 := ManifestRec{
			partitionId: pNum,
			seqNum:      inactiveLogDetails.FileSeqNum,
			fop:         defaultConstants.fileDelete,
			fileType:    defaultConstants.logFileType,
		}

		mf2 := ManifestRec{
			partitionId: pNum,
			levelNum:    0,
			seqNum:      pInfo.sstWriter.SeqNum,
			fop:         defaultConstants.fileCreate,
			fileType:    defaultConstants.sstFileType,
		}
		//write log-delete and sst-create details
		write([]ManifestRec{mf1, mf2})

		//delete 0th pos
		if len(pInfo.inactiveLogDetails) > 1 {
			pInfo.readLock.Lock()
			pInfo.inactiveLogDetails = append(pInfo.inactiveLogDetails[:0], pInfo.inactiveLogDetails[1:]...)
			pInfo.readLock.Unlock() //TODO - defer in a loop
		}

		//check for possible compaction
		pInfo.levelLock.RLock()
		levelInfo := pInfo.levelsInfo[0]
		if len(levelInfo.sstSeqNums) >= 4 {
			compactQueue <- &CompactInfo{
				partitionId: pNum,
				thisLevel:   0,
			}
		}
		pInfo.levelLock.RUnlock()
		MemFlushQueueWG.Done()
	}
}

//Stop reading while flushing data to sst, sst recs might not be available before it exits in indexRec
func (pInfo *PartitionInfo) writeSSTAndIndex(memRecs *sklist.SkipList) {
	//sstReader meta
	var startKey []byte = nil
	var endKey []byte = nil
	var noOfDelReq uint64 = 0
	var noOfWriteReq uint64 = 0
	//flush memRecs to SST and index
	iterator := memRecs.NewIterator()
	for iterator.Next() {
		value := iterator.Value().Value().(*memstore.MemRec)
		indexRec := &IndexRec{
			SSTRecOffset:  pInfo.sstWriter.Offset,
			SSTFileSeqNum: pInfo.sstWriter.SeqNum,
			TS:            value.TS,
		}
		_, sstErr := pInfo.sstWriter.Write(value.Key, value.Value, value.TS, value.RecType)
		if sstErr != nil {
			panic(sstErr)
		}

		//if rec type is writeReq then load to index, delete request need not load to index
		if value.RecType == defaultConstants.writeReq {
			//indexRec.SSTRecLen = bytesWritten
			keyHash, _ := GetHash(value.Key)
			pInfo.index.Set(keyHash, indexRec)
			noOfWriteReq++
		} else {
			noOfDelReq++
		}

		if startKey == nil {
			startKey = value.Key
		}
		endKey = value.Key
	}

	//pInfo.index.List() //remove this line - test
	flushedFileSeqNum, err := pInfo.sstWriter.FlushAndClose()

	if err != nil {
		fmt.Println("Error while flushing data sst")
		panic(err)
	}

	//update sstReader map
	pInfo.readLock.Lock()
	pInfo.levelLock.Lock()
	defer pInfo.levelLock.Unlock()
	defer pInfo.readLock.Unlock()

	levelNum := 0
	levelInfo := pInfo.levelsInfo[levelNum]
	if levelInfo == nil { //TODO - remove this after test
		panic(fmt.Sprintf("levelInfo is nil for levelNum: %d", levelNum))
		//pInfo.levelsInfo[levelNum] = &LevelInfo{
		//	SSTReaderMap: make(map[uint32]*SSTReader),
		//}
		//levelInfo = pInfo.levelsInfo[levelNum]
	}

	sstReader, err := NewSSTReader(flushedFileSeqNum, pInfo.partitionId)
	if err != nil {
		fmt.Println(fmt.Sprintf("Failed to update reader map for sstFileSeqNum: %d", flushedFileSeqNum))
		panic(err)
	}

	sstReader.startKey = startKey
	sstReader.endKey = endKey
	sstReader.noOfWriteReq = noOfWriteReq
	sstReader.noOfDelReq = noOfDelReq
	levelInfo.sstSeqNums[flushedFileSeqNum] = struct{}{}
	pInfo.sstReaderMap[flushedFileSeqNum] = sstReader
}

/*//writes the unclosed wal file to new wal file and memtable
func (pInfo *PartitionInfo) loadUnclosedLogFile() {
	unClosedWalFiles, err := GetUnClosedWalFile(pInfo.partitionId)

	if err != nil {
		panic(err)
		return
	}

	if len(unClosedWalFiles) == 0 {
		fmt.Println(fmt.Sprintf("Found No unclsoed wal file for partition: %d", pInfo.partitionId))
		return
	}
	var recsRecovered = 0
	var recoveredBytes uint32 = 0

	var totalRecoveredBytes uint32 = 0
	var totalRecsRecovered = 0

	for _, unClosedWalFile := range unClosedWalFiles {
		recsRecovered = 0
		recoveredBytes = 0

		walFile, err := os.OpenFile(unClosedWalFile, os.O_RDWR, 0644)
		if err != nil {
			fmt.Println(fmt.Sprintf("Error while opening wal file during setting uo db, fileName: %s", unClosedWalFile))
			panic(err)
			return
		}
		memTable, err := memstore.NewMemStore(pInfo.partitionId)
		if err != nil {
			fmt.Println(fmt.Sprintf("Error while getting new memstore during setting uo db, partitionId: %d", pInfo.partitionId))
			panic(err)
			return
		}

		for {
			var headerBuf = make([]byte, defaultConstants.logRecLen)
			n, _ := walFile.Read(headerBuf[:])
			if n == 0 { //end of file
				break;
			}

			var recLength = binary.LittleEndian.Uint32(headerBuf[:])
			var recBuf = make([]byte, recLength)
			walFile.Read(recBuf[:])

			var walRec = LogRecord{}
			gotiny.Unmarshal(recBuf[:], &walRec)

			memTable.Insert(walRec.Key(), walRec.Value(), walRec.header.ts, walRec.header.recType)

			recsRecovered++
			recoveredBytes += defaultConstants.logRecLen + recLength
		}

		if recoveredBytes == 0 {
			fmt.Printf("recovered 0 bytes from %s unclosed wal file, marking it for delete", unClosedWalFile)
			pInfo.logStatWriter.AddCheckpoint(pInfo.partitionId, unClosedWalFile,
				defaultConstants.logFileStartOffset, defaultConstants.fileDelete)
			continue
		}
		totalRecoveredBytes += recoveredBytes

		inactiveLogDetails := &InactiveLogDetails{
			FileName:    unClosedWalFile,
			WriteOffset: recoveredBytes,
			MemTable:    memTable,
			PartitionId: pInfo.partitionId,
		}
		pInfo.readLock.Lock()
		pInfo.inactiveLogDetails = append(pInfo.inactiveLogDetails, inactiveLogDetails)
		pInfo.readLock.Unlock()
		fmt.Println(fmt.Sprintf("Recovered %d recs %d bytes from %s unclosed wal file", recsRecovered,
			recoveredBytes, unClosedWalFile))
	}
	fmt.Println(fmt.Sprintf("Total recs recovered %d, %d bytes ", totalRecsRecovered, totalRecoveredBytes))
}*/
