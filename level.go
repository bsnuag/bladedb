package bladedb

import (
	"fmt"
)

//TODO = do we need a lock here ..?
//this map is always accessed while reading or when mem-table gets flushed

type LevelInfo struct {
	SSTReaderMap map[uint32]SSTReader //sst_seq_num - SSTReader
}

func loadPartitionData(partId int) (uint32, error) {
	maxSSTSeq := uint32(0)
	if manifestFile.manifest == nil { //before this run manifest replay()
		fmt.Println("no manifest info to be loaded")
		return maxSSTSeq, nil
	}
	partitionInfo := partitionInfoMap[partId]
	levelInfos := partitionInfo.levelsInfo

	for _, manifestRec := range manifestFile.manifest.sstManifest[partId].manifestRecs {
		if manifestRec.seqNum > maxSSTSeq {
			maxSSTSeq = manifestRec.seqNum
		}
		if manifestRec.fop == defaultConstants.fileDelete { //file delete req
			delete(levelInfos[manifestRec.levelNum].SSTReaderMap, manifestRec.seqNum)
			deleteSST(partId, manifestRec.seqNum)
		} else if manifestRec.fop == defaultConstants.fileCreate {
			reader, err := NewSSTReader(manifestRec.seqNum, partId)
			if err != nil {
				panic(err)
				return maxSSTSeq, err
			}
			levelInfos[manifestRec.levelNum].SSTReaderMap[manifestRec.seqNum] = reader
			(&reader).loadSSTRec(partitionInfo.index)
		} else {
			fmt.Println(fmt.Sprintf("invalid manifest file operations: %d, accepts only 0 and 1", manifestRec.fop))
		}
	}
	return maxSSTSeq, nil
}
