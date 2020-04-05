package bladedb

import (
	"crypto/sha256"
	"fmt"
	"github.com/spaolacci/murmur3"
	"sort"
	"time"
)

func GetHash(input []byte) (string, error) {
	h := sha256.New()
	_, err := h.Write(input)
	if err != nil {
		panic(err)
		return "", err
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func GetPartitionId(key interface{}) int {
	switch v := key.(type) {
	default:
		fmt.Printf("unexpected type %T", v)
		panic("Error while getting partitionId")
	case []byte:
		return int(murmur3.Sum64(key.([]byte)) % uint64(DefaultConstants.noOfPartitions))
	case string:
		return int(murmur3.Sum64([]byte(key.(string))) % uint64(DefaultConstants.noOfPartitions))
	}
}

func newLevelInfo() map[int]*LevelInfo {
	levelsInfo := make(map[int]*LevelInfo)
	for lNo := 0; lNo <= DefaultConstants.maxLevel; lNo++ {
		levelsInfo[lNo] = &LevelInfo{
			sstSeqNums: make(map[uint32]struct{}),
		}
	}
	return levelsInfo
}

func NanoTime() uint64 {
	return uint64(time.Now().UnixNano()) / 1000
}

func sortedKeys(inMap map[uint32]SSTReader) []uint32 {
	sortedKeys := make([]uint32, 0, len(inMap))
	for k := range inMap {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Slice(sortedKeys, func(i, j int) bool { return sortedKeys[i] < sortedKeys[j] })
	return sortedKeys
}
