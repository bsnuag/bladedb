package bladedb

import (
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

	pInfo.levelLock.RLock()

	levelInfo := pInfo.levelsInfo[indexRec.SSTFileLevel]
	if levelInfo == nil {
		fmt.Printf("Could not find %d level info in partition\n", indexRec.SSTFileLevel)
		return nil
	}
	var sstReader SSTReader = SSTReader{}
	if _, ok := levelInfo.SSTReaderMap[indexRec.SSTFileSeqNum]; ok {
		sstReader = levelInfo.SSTReaderMap[indexRec.SSTFileSeqNum]
	} else {
		fmt.Printf("Could not find sstReader for seqNum %d under level %d in partition\n",
			indexRec.SSTFileSeqNum, indexRec.SSTFileLevel)
		return nil
	}
	stableRec, err := sstReader.ReadRec(int64(indexRec.SSTRecOffset))

	pInfo.levelLock.RUnlock()

	if err != nil {
		fmt.Println(fmt.Sprintf("error while reading sst record for indexRec: %v", indexRec))
		panic(err)
		return nil
	}
	fmt.Println(fmt.Sprintf("sstRec: %v", stableRec))
	fmt.Println("---------------------------------------")
	return nil
}
