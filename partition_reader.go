package bladedb

import (
	"fmt"
	"github.com/pkg/errors"
)

func Get(key []byte) ([]byte, error) {
	hash := Hash(key)
	partitionId := PartitionId(hash[:])
	pInfo := partitionInfoMap[partitionId]

	pInfo.readLock.RLock()
	defer pInfo.readLock.RUnlock()

	memRec, _ := pInfo.memTable.Find(key)

	if memRec != nil {
		if memRec.RecType == DefaultConstants.deleteReq { //if delete request
			return nil, nil
		}
		return memRec.Value, nil
	}

	//Loop inactive memtable in reverse(later one is most updated one), if rec found return
	i := len(pInfo.inactiveLogDetails) - 1
	for ; i >= 0; i-- {
		inactiveLog := pInfo.inactiveLogDetails[i]
		memRec, err := inactiveLog.MemTable.Find(key)
		if err != nil {
			return nil, errors.Wrapf(err, "Error while reading data from inactive memtables for key: %s", key)
		}
		if memRec != nil {
			if memRec.RecType == DefaultConstants.deleteReq {
				return nil, nil
			}
			return memRec.Value, nil
		}
	}

	if indexRec, ok := pInfo.index.Get(hash); ok {
		stableRec, err := pInfo.getFromSST(indexRec.SSTFileSeqNum, indexRec.SSTRecOffset)

		if err != nil {
			return nil, errors.Wrapf(err, "error while reading sst record for indexRec: %v", indexRec)
		}
		return stableRec.value, nil
	}
	return nil, nil
}

func (pInfo *PartitionInfo) getFromSST(sNum uint32, offset uint32) (SSTRec, error) {
	pInfo.levelLock.RLock()
	defer pInfo.levelLock.RUnlock()

	sstReader, ok := pInfo.sstReaderMap[sNum]
	if !ok {
		return SSTRec{}, errors.New(fmt.Sprintf("Could not find sstReader for seqNum %d in %d partition",
			sNum, pInfo.partitionId))
	}
	n, sstRec := sstReader.ReadRec(offset)
	if n == 0 {
		return SSTRec{}, errors.New(fmt.Sprintf("Could not find data for offset %d in seqNum %d in %d partition",
			offset, sNum, pInfo.partitionId))
	}
	return sstRec, nil
}
