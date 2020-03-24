package bladedb

import (
	"fmt"
	"github.com/pkg/errors"
)

func Get(key string) ([]byte, error) {
	keyByte := []byte(key)
	keyHash, _ := GetHash(keyByte)
	partitionId := GetPartitionId(keyHash)
	pInfo := partitionInfoMap[partitionId]

	pInfo.readLock.RLock()
	defer pInfo.readLock.RUnlock()

	memRec, _ := pInfo.memTable.Find(keyByte)

	if memRec != nil {
		if memRec.RecType == DefaultConstants.deleteReq { //if delete request
			return nil, nil
		}
		return memRec.Value, nil
	}

	//Loop inactive memtable in reverse(later one is most updated one), if rec found return
	last := len(pInfo.inactiveLogDetails)
	for i := range pInfo.inactiveLogDetails {
		inactiveLog := pInfo.inactiveLogDetails[last-i]
		if memRec, err := inactiveLog.MemTable.Find(keyByte); err != nil && memRec != nil {
			if memRec.RecType == DefaultConstants.deleteReq {
				return nil, nil
			}
			return memRec.Value, nil
		}
	}
	var indexRec *IndexRec = nil
	if indexVal := pInfo.index.Get(keyHash); indexVal != nil {
		indexRec = (indexVal.Value()).(*IndexRec)
	}
	if indexRec == nil {
		return nil, nil
	}

	stableRec, err := pInfo.getFromSST(indexRec.SSTFileSeqNum, indexRec.SSTRecOffset)

	if err != nil {
		return nil, errors.Wrapf(err, "error while reading sst record for indexRec: %v", indexRec)
	}
	return stableRec.val, nil
}

func (pInfo *PartitionInfo) getFromSST(sNum uint32, offset uint32) (*SSTRec, error) {
	pInfo.levelLock.RLock()
	defer pInfo.levelLock.RUnlock()

	var sstReader SSTReader = SSTReader{}
	if _, ok := pInfo.sstReaderMap[sNum]; ok {
		sstReader = pInfo.sstReaderMap[sNum]
	} else {
		return nil, errors.New(fmt.Sprintf("Could not find sstReader for seqNum %d in %d partition",
			sNum, pInfo.partitionId))
	}
	return sstReader.ReadRec(int64(offset))
}
