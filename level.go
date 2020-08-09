package bladedb

//this map is always accessed while reading or when mem-table gets flushed

type LevelInfo struct {
	sstSeqNums map[uint32]struct{}
}

func (pInfo *PartitionInfo) fillLevelInfo(manifest *Manifest) (uint32, error) {
	maxSSTSeq := uint32(0)
	if manifest == nil {
		db.logger.Info().Msgf("No SSTs to load for partition: %d", pInfo.partitionId)
		return maxSSTSeq, nil
	}

	for _, manifestRec := range manifest.sstManifest[pInfo.partitionId].manifestRecs {
		if manifestRec.seqNum > maxSSTSeq {
			maxSSTSeq = manifestRec.seqNum
		}
		if manifestRec.fop == fDelete {
			delete(pInfo.levelsInfo[manifestRec.levelNum].sstSeqNums, manifestRec.seqNum)
			deleteSST(pInfo.partitionId, manifestRec.seqNum)
		} else if manifestRec.fop == fCreate {
			pInfo.levelsInfo[manifestRec.levelNum].sstSeqNums[manifestRec.seqNum] = struct{}{}
		} else {
			db.logger.Fatal().Msgf("unsupported manifest file operations: %d, accepts only 0 & 1", manifestRec.fop)
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
					fop:         fDelete,
					fileType:    DataFileType,
				}
				writeManifest([]ManifestRec{mf1})
			} else if err != nil {
				return maxSSTSeq, err
			}
			pInfo.sstReaderMap[sstSeqNum] = &reader
		}
	}
	return maxSSTSeq, nil
}
