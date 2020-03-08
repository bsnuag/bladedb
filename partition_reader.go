package bladedb

import (
	"errors"
	"fmt"
)

func Get(key string) []byte {
	keyByte := []byte(key)
	keyHash, _ := GetHash(keyByte)
	partitionId := GetPartitionId(keyHash)
	pInfo := partitionInfoMap[partitionId]

	pInfo.readLock.RLock()
	defer pInfo.readLock.RUnlock()

	//fmt.Println(fmt.Sprintf("read req for key: %s", key))

	memRec, _ := pInfo.memTable.Find(keyByte)

	if memRec != nil {
		if memRec.RecType == defaultConstants.deleteReq { //if delete request
			return nil
		}
		return memRec.Value
	}

	//Loop inactive memtable in reverse(later one is most updated one), if rec found return
	last := len(pInfo.inactiveLogDetails)
	for i := range pInfo.inactiveLogDetails {
		inactiveLog := pInfo.inactiveLogDetails[last-i]
		if memRec, err := inactiveLog.MemTable.Find(keyByte); err != nil && memRec != nil {
			if memRec.RecType == defaultConstants.deleteReq {
				return nil
			}
			return memRec.Value
		}
	}
	var indexRec *IndexRec = nil
	if indexVal := pInfo.index.Get(keyHash); indexVal != nil {
		indexRec = (indexVal.Value()).(*IndexRec)
	}
	if indexRec == nil {
		return nil
	}

	stableRec, err := pInfo.getFromSST(indexRec.SSTFileSeqNum, indexRec.SSTRecOffset)

	if err != nil {
		fmt.Println(fmt.Sprintf("error while reading sst record for indexRec: %v", indexRec))
		panic(err)
		return nil
	}
	fmt.Println(fmt.Sprintf("sstRec: %v", stableRec))
	fmt.Println("---------------------------------------")
	return nil
}

func (pInfo *PartitionInfo) getFromSST(sNum uint32, offset uint32) (*SSTRec, error) {
	pInfo.levelLock.RLock()
	defer pInfo.levelLock.RUnlock()

	var sstReader SSTReader = SSTReader{}
	if _, ok := pInfo.sstReaderMap[sNum]; ok {
		sstReader = pInfo.sstReaderMap[offset]
	} else {
		return nil, errors.New(fmt.Sprintf("Could not find sstReader for seqNum %d in %d partition",
			sNum, pInfo.partitionId))
	}
	return sstReader.ReadRec(int64(offset))
}
