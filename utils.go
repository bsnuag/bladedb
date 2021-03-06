package bladedb

import (
	"crypto/sha256"
	"encoding/binary"
	"os"
	"sort"
	"time"
)

func Hash(input []byte) [32]byte { //thread safe ?
	return sha256.Sum256(input)
}

func PartitionId(input []byte) int {
	return int(binary.LittleEndian.Uint16(input[:2]) % uint16(db.config.NoOfPartitions))
}

func newLevelInfo() map[int]*LevelInfo {
	levelsInfo := make(map[int]*LevelInfo)
	for lNo := 0; lNo <= MaxLevel; lNo++ {
		levelsInfo[lNo] = &LevelInfo{
			sstSeqNums: make(map[uint32]struct{}),
		}
	}
	return levelsInfo
}

func NanoTime() uint64 {
	return uint64(time.Now().UnixNano()) / 1000
}

func sortedKeys(inMap map[uint32]*SSTReader) []uint32 {
	sortedKeys := make([]uint32, 0, len(inMap))
	for k := range inMap {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Slice(sortedKeys, func(i, j int) bool { return sortedKeys[i] < sortedKeys[j] })
	return sortedKeys
}

func fileSize(file string) (int64, error) {
	stat, err := os.Stat(file)
	if err != nil {
		return 0, err
	} else if stat.Size() == 0 {
		return 0, EmptyFile
	}
	return stat.Size(), nil
}
