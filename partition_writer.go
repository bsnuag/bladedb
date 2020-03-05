package bladedb

import (
	"bladedb/memstore"
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
	publishMemFlushTask(inactiveLogDetails)
}
