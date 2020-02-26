package bladedb

import (
	"fmt"
)

//TODO = do we need a lock here ..?
//this map is always accessed while reading or when mem-table gets flushed

type LevelInfo struct { //SSTReaderMap map[uint32]SSTReader //sst_seq_num - SSTReader
	sstSeqNums map[uint32]struct{}
}

func loadPartitionData(partId int) (uint32, error) {
	maxSSTSeq := uint32(0)
	if manifestFile.manifest == nil { //before this run manifest replay()
		fmt.Println("no manifest info to be loaded")
		return maxSSTSeq, nil
	}
	partitionInfo := partitionInfoMap[partId]

	for _, manifestRec := range manifestFile.manifest.sstManifest[partId].manifestRecs {
		if manifestRec.seqNum > maxSSTSeq {
			maxSSTSeq = manifestRec.seqNum
		}
		if manifestRec.fop == defaultConstants.fileDelete { //file delete req
			delete(partitionInfo.levelsInfo[manifestRec.levelNum].sstSeqNums, manifestRec.seqNum)
			deleteSST(partId, manifestRec.seqNum)
		} else if manifestRec.fop == defaultConstants.fileCreate {
			partitionInfo.levelsInfo[manifestRec.levelNum].sstSeqNums[manifestRec.seqNum] = struct{}{}
		} else {
			fmt.Println(fmt.Sprintf("invalid manifest file operations: %d, accepts only 0 and 1", manifestRec.fop))
		}
	}
	for _, levelInfo := range partitionInfo.levelsInfo {
		for sstSeqNum := range levelInfo.sstSeqNums {
			reader, err := NewSSTReader(sstSeqNum, partId)
			if err != nil {
				panic(err)
				return maxSSTSeq, err
			}
			(&reader).loadSSTRec(partitionInfo.index)
			partitionInfo.sstReaderMap[sstSeqNum] = reader
		}
	}
	return maxSSTSeq, nil
}
