package bladedb

import (
	"bladedb/memstore"
	"fmt"
	"github.com/pkg/errors"
)

func Remove(key []byte) (value []byte, err error) {
	hash := Hash(key)
	partitionId := PartitionId(hash[:])
	pInfo := partitionInfoMap[partitionId]

	if pInfo == nil {
		return value, errors.New(fmt.Sprintf("partition doesn't exists for partition: %d, Key: %s ", partitionId, key))
	}

	pInfo.writeLock.Lock()
	defer pInfo.writeLock.Unlock()

	value, err = Get(key)
	if err != nil {
		return value, errors.Wrapf(err, "error while deleting key: %s", key)
	}
	ts := NanoTime()
	inactiveLogDetails, err := pInfo.logWriter.Write(key, nil, ts, DefaultConstants.deleteReq)

	if err != nil {
		return value, errors.Wrapf(err, "error while deleting key: %s", key)
	}

	if inactiveLogDetails != nil && inactiveLogDetails.WriteOffset > 0 {
		pInfo.handleRolledOverLogDetails(inactiveLogDetails)
	}

	//pInfo.readLock.Lock() - skiplist is thread-safe
	pInfo.memTable.Insert(key, nil, ts, DefaultConstants.deleteReq) //We need to put delete rec in mem, since it gets into SST later
	pInfo.index.Remove(hash)
	//pInfo.readLock.Unlock() - skiplist is thread-safe
	return value, nil
}

func Put(key []byte, valueByte []byte) error {
	hash := Hash(key)
	partitionId := PartitionId(hash[:])
	pInfo := partitionInfoMap[partitionId]

	if pInfo == nil {
		return errors.New(fmt.Sprintf("partition doesn't exists for partition: %d, Key: %s ", partitionId, key))
	}

	//fmt.Println(fmt.Sprintf("Write Req for Key: %s, partId: %d", key, partitionId))
	pInfo.writeLock.Lock()
	defer pInfo.writeLock.Unlock()

	ts := NanoTime()
	inactiveLogDetails, err := pInfo.logWriter.Write(key, valueByte, ts, DefaultConstants.writeReq)

	if err != nil {
		return errors.Wrap(err, "error while writing into db")
	}

	if inactiveLogDetails != nil && inactiveLogDetails.WriteOffset > 0 {
		pInfo.handleRolledOverLogDetails(inactiveLogDetails)
	}

	//pInfo.readLock.Lock()- skiplist is thread-safe
	pInfo.memTable.Insert(key, valueByte, ts, DefaultConstants.writeReq)
	//pInfo.readLock.Unlock()- skiplist is thread-safe
	return nil
}

func (pInfo *PartitionInfo) handleRolledOverLogDetails(inactiveLogDetails *InactiveLogDetails) {
	pInfo.readLock.Lock()
	defer pInfo.readLock.Unlock()

	fmt.Println("inactiveLogDetails: ", inactiveLogDetails)

	oldMemTable := pInfo.memTable

	memTable, err := memstore.NewMemStore()
	if err != nil {
		panic(err)
		return
	}
	pInfo.memTable = memTable //update active memtable to a new one

	inactiveLogDetails.MemTable = oldMemTable
	pInfo.inactiveLogDetails = append(pInfo.inactiveLogDetails, inactiveLogDetails)
	publishMemFlushTask(inactiveLogDetails)
}
