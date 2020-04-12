package bladedb

import (
	"fmt"
)

//this map is always accessed while reading or when mem-table gets flushed

type LevelInfo struct {
	//SSTReaderMap map[uint32]SSTReader //sst_seq_num - SSTReader
	sstSeqNums map[uint32]struct{}
}

func (pInfo *PartitionInfo) loadActiveSSTs() (uint32, error) {
	maxSSTSeq := uint32(0)
	if manifestFile.manifest == nil { //before this run manifest replay()
		fmt.Println("no manifest info to be loaded")
		return maxSSTSeq, nil
	}

	for _, manifestRec := range manifestFile.manifest.sstManifest[pInfo.partitionId].manifestRecs {
		if manifestRec.seqNum > maxSSTSeq {
			maxSSTSeq = manifestRec.seqNum
		}
		if manifestRec.fop == DefaultConstants.fileDelete { //file delete req
			delete(pInfo.levelsInfo[manifestRec.levelNum].sstSeqNums, manifestRec.seqNum)
			deleteSST(pInfo.partitionId, manifestRec.seqNum)
		} else if manifestRec.fop == DefaultConstants.fileCreate {
			pInfo.levelsInfo[manifestRec.levelNum].sstSeqNums[manifestRec.seqNum] = struct{}{}
		} else {
			fmt.Println(fmt.Sprintf("invalid manifest file operations: %d, accepts only 0 and 1", manifestRec.fop))
		}
	}
	for levelNum, levelInfo := range pInfo.levelsInfo {
		for sstSeqNum := range levelInfo.sstSeqNums {
			reader, err := NewSSTReader(sstSeqNum, pInfo.partitionId)
			if err == EmptyFile {
				mf1 := ManifestRec{
					partitionId: pInfo.partitionId,
					levelNum:    levelNum,
					seqNum:      sstSeqNum,
					fop:         DefaultConstants.fileDelete,
					fileType:    DefaultConstants.sstFileType,
				}
				writeManifest([]ManifestRec{mf1})
			} else if err != nil {
				panic(err)
				return maxSSTSeq, err
			}
			pInfo.sstReaderMap[sstSeqNum] = reader
		}
	}
	return maxSSTSeq, nil
}
