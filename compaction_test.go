package bladedb

import (
	"bufio"
	"fmt"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"
)

func TestKeyRange(t *testing.T) {
	min, max := keyRange("111", "199", "222", "299")
	require.Equal(t, min, "111", "111 should be min")
	require.Equal(t, max, "299", "299 should be max")

	min, max = keyRange("222", "299", "111", "199")
	require.Equal(t, min, "111", "111 should be min")
	require.Equal(t, max, "299", "299 should be max")

	min, max = keyRange("122", "188", "111", "199")
	require.Equal(t, min, "111", "111 should be min")
	require.Equal(t, max, "199", "199 should be max")

	min, max = keyRange("233", "255", "222", "299")
	require.Equal(t, min, "222", "222 should be min")
	require.Equal(t, max, "299", "299 should be max")
}

func TestFillLevels_level0WithOverlap(t *testing.T) {
	partId := 0
	levelsInfo := newLevelInfo()
	info_0 := levelsInfo[0] //level - 0

	info_0.SSTReaderMap[123] = tempSSTReader("200", "250", 123, 0, 12)
	info_0.SSTReaderMap[124] = tempSSTReader("270", "300", 124, 0, 14)
	info_0.SSTReaderMap[125] = tempSSTReader("998", "999", 125, 0, 101)

	info_1 := levelsInfo[1] //level - 1

	info_1.SSTReaderMap[234] = tempSSTReader("222", "666", 234, 0, 100)

	pInfo := &PartitionInfo{
		partitionId: partId,
		levelsInfo:  levelsInfo,
	}

	partitionInfoMap[0] = pInfo

	compactInfo := initCompactInfo(0, partId)
	sKey, eKey := compactInfo.fillLevels()

	/*fmt.Println("----")

	fmt.Println("compaction selected key-range -> "+sKey+" : ", eKey)

	fmt.Println("\nbot level SST infos - ")
	for _, reader := range compactInfo.botLevelSST {
		fmt.Println("\nseqNum - ", reader.SeqNm)
		fmt.Println("sKey - " + string(reader.startKey))
		fmt.Println("eKey - " + string(reader.endKey))
	}
	fmt.Println("\ntop level SST infos - ")
	for _, reader := range compactInfo.topLevelSST {
		fmt.Println("\nseqNum - ", reader.SeqNm)
		fmt.Println("sKey - " + string(reader.startKey))
		fmt.Println("eKey - " + string(reader.endKey))
	}*/
	require.Equal(t, "200", sKey, "start key should be 200")
	require.Equal(t, "300", eKey, "start key should be 300")
	require.Equal(t, 2, len(compactInfo.botLevelSST), "2 ssts should be picked up from bottom level")
	require.Equal(t, 1, len(compactInfo.topLevelSST), "1 sst should be picked up from top level")
	require.Equal(t, 0, compactInfo.heap.Len(), "heap length should be zero")
}

func TestFillLevels_level0WithNoLevel1Data(t *testing.T) {
	partId := 0
	levelsInfo := make(map[int]*LevelInfo)
	for l := 0; l <= defaultConstants.maxLevel; l++ {
		if levelsInfo[l] == nil {
			levelsInfo[l] = &LevelInfo{
				SSTReaderMap: make(map[uint32]SSTReader),
			}
		}
	}
	info_0 := levelsInfo[0] //level - 0

	info_0.SSTReaderMap[123] = tempSSTReader("200", "250", 123, 0, 12)
	info_0.SSTReaderMap[124] = tempSSTReader("270", "300", 124, 0, 14)
	info_0.SSTReaderMap[125] = tempSSTReader("998", "999", 125, 0, 101)

	pInfo := &PartitionInfo{
		partitionId: partId,
		levelsInfo:  levelsInfo,
	}

	//fmt.Println(pInfo.levelsInfo[0])
	//fmt.Println(pInfo.levelsInfo[1])
	partitionInfoMap[0] = pInfo

	compactInfo := initCompactInfo(0, partId)
	sKey, eKey := compactInfo.fillLevels()

	//fmt.Println("----")
	//
	//fmt.Println("compaction selected key-range -> "+sKey+" : ", eKey)
	//
	//fmt.Println("\nbot level SST infos - ")
	//for _, reader := range compactInfo.botLevelSST {
	//	fmt.Println("\nseqNum - ", reader.SeqNm)
	//	fmt.Println("sKey - " + string(reader.startKey))
	//	fmt.Println("eKey - " + string(reader.endKey))
	//}
	//fmt.Println("\ntop level SST infos - ")
	//for _, reader := range compactInfo.topLevelSST {
	//	fmt.Println("\nseqNum - ", reader.SeqNm)
	//	fmt.Println("sKey - " + string(reader.startKey))
	//	fmt.Println("eKey - " + string(reader.endKey))
	//}

	require.Equal(t, "200", sKey, "start key should be 200")
	require.Equal(t, "999", eKey, "start key should be 999")
	require.Equal(t, 3, len(compactInfo.botLevelSST), "bot level should be 3 sst")
	require.Equal(t, 0, len(compactInfo.topLevelSST), "top level should be 0 sst")

}

func TestFillLevels_level1WithNoLevel2Data(t *testing.T) {
	partId := 0
	levelsInfo := newLevelInfo()
	info_1 := levelsInfo[1] //Level 1 have only 3 sst which is lower than levelMaxSST[1] - this should be taken care when compactionInfo is being pushed to compactQueue

	info_1.SSTReaderMap[123] = tempSSTReader("200", "250", 123, 7, 12)
	info_1.SSTReaderMap[124] = tempSSTReader("270", "300", 124, 0, 14)
	info_1.SSTReaderMap[125] = tempSSTReader("998", "999", 125, 40, 101)

	pInfo := &PartitionInfo{
		partitionId: partId,
		levelsInfo:  levelsInfo,
	}

	partitionInfoMap[partId] = pInfo

	compactInfo := initCompactInfo(1, partId)
	sKey, eKey := compactInfo.fillTopLevels()

	/*fmt.Println("----")

	fmt.Println("compaction selected key-range -> "+sKey+" : ", eKey)

	fmt.Println("\nbot level SST infos - ")
	for _, reader := range compactInfo.botLevelSST {
		fmt.Println("\nseqNum - ", reader.SeqNm)
		fmt.Println("sKey - " + string(reader.startKey))
		fmt.Println("eKey - " + string(reader.endKey))
	}
	fmt.Println("\ntop level SST infos - ")
	for _, reader := range compactInfo.topLevelSST {
		fmt.Println("\nseqNum - ", reader.SeqNm)
		fmt.Println("sKey - " + string(reader.startKey))
		fmt.Println("eKey - " + string(reader.endKey))
	}*/
	require.Equal(t, "", sKey, "level 1 has only 3 files i.e. less than levelMaxSST, compaction not possible")
	require.Equal(t, "", eKey, "level 1 has only 3 files i.e. less than levelMaxSST, compaction not possible")
	require.Equal(t, 0, len(compactInfo.botLevelSST), "level 1 has only 3 files i.e. less than levelMaxSST, no file should be picked up for compaction")
	require.Equal(t, 0, len(compactInfo.topLevelSST), "level 1 has only 3 files i.e. less than levelMaxSST, no file should be picked up for compaction")
	require.Equal(t, 0, len(compactInfo.heap), "level 1 has only 3 files i.e. less than levelMaxSST, heap length should be zero")
	require.Equal(t, 0, compactInfo.idx.Length, "level 1 has only 3 files i.e. less than levelMaxSST, index length should be zero")
}

func TestFillLevels_level1WithOverlap(t *testing.T) {
	partId := 0
	level := 1
	defaultConstants.levelMaxSST[level] = 2
	levelsInfo := newLevelInfo()
	info_0 := levelsInfo[level]

	info_0.SSTReaderMap[123] = tempSSTReader("200", "250", 123, 0, 12)
	info_0.SSTReaderMap[124] = tempSSTReader("270", "300", 124, 0, 14)
	info_0.SSTReaderMap[125] = tempSSTReader("998", "999", 125, 0, 101)

	info_1 := levelsInfo[level+1]

	info_1.SSTReaderMap[234] = tempSSTReader("222", "666", 234, 0, 100)

	pInfo := &PartitionInfo{
		partitionId: partId,
		levelsInfo:  levelsInfo,
	}

	partitionInfoMap[partId] = pInfo

	compactInfo := initCompactInfo(level, partId)
	sKey, eKey := compactInfo.fillTopLevels()

	/*fmt.Println("----")

	fmt.Println("compaction selected key-range -> "+sKey+" : ", eKey)

	fmt.Println("\nbot level SST infos - ")
	for _, reader := range compactInfo.botLevelSST {
		fmt.Println("\nseqNum - ", reader.SeqNm)
		fmt.Println("sKey - " + string(reader.startKey))
		fmt.Println("eKey - " + string(reader.endKey))
	}
	fmt.Println("\ntop level SST infos - ")
	for _, reader := range compactInfo.topLevelSST {
		fmt.Println("\nseqNum - ", reader.SeqNm)
		fmt.Println("sKey - " + string(reader.startKey))
		fmt.Println("eKey - " + string(reader.endKey))
	}*/
	require.Equal(t, "200", sKey, "start key should be 200")
	require.Equal(t, "300", eKey, "start key should be 300")
	require.Equal(t, 2, len(compactInfo.botLevelSST), "2 ssts should be picked up from bottom level")
	require.Equal(t, 1, len(compactInfo.topLevelSST), "1 sst should be picked up from top level")
	require.Equal(t, 0, compactInfo.heap.Len(), "heap length should be zero")
}

func TestFillLevels_level1WithOverlapButNoLevel2Data(t *testing.T) {
	partId := 0
	level := 1
	defaultConstants.levelMaxSST[level] = 1
	levelsInfo := newLevelInfo()
	info_0 := levelsInfo[level]

	info_0.SSTReaderMap[123] = tempSSTReader("200", "250", 123, 0, 12)
	info_0.SSTReaderMap[124] = tempSSTReader("270", "300", 124, 0, 14)
	info_0.SSTReaderMap[125] = tempSSTReader("998", "999", 125, 0, 101)

	info_1 := levelsInfo[level+1]

	info_1.SSTReaderMap[234] = tempSSTReader("000", "199", 234, 0, 100)

	pInfo := &PartitionInfo{
		partitionId: partId,
		levelsInfo:  levelsInfo,
	}

	partitionInfoMap[partId] = pInfo

	compactInfo := initCompactInfo(level, partId)
	sKey, eKey := compactInfo.fillTopLevels()

	/*fmt.Println("----")

	fmt.Println("compaction selected key-range -> "+sKey+" : ", eKey)

	fmt.Println("\nbot level SST infos - ")
	for _, reader := range compactInfo.botLevelSST {
		fmt.Println("\nseqNum - ", reader.SeqNm)
		fmt.Println("sKey - " + string(reader.startKey))
		fmt.Println("eKey - " + string(reader.endKey))
	}
	fmt.Println("\ntop level SST infos - ")
	for _, reader := range compactInfo.topLevelSST {
		fmt.Println("\nseqNum - ", reader.SeqNm)
		fmt.Println("sKey - " + string(reader.startKey))
		fmt.Println("eKey - " + string(reader.endKey))
	}*/
	require.Equal(t, "200", sKey, "start key should be 200")
	require.Equal(t, "999", eKey, "start key should be 999")
	require.Equal(t, 3, len(compactInfo.botLevelSST), "3 ssts should be picked up from bottom level")
	require.Equal(t, 0, len(compactInfo.topLevelSST), "0 sst should be picked up from top level")
	require.Equal(t, 0, compactInfo.heap.Len(), "heap length should be zero")
}
func tempSSTReader(sKey string, eKey string, seqNum uint32, delReq uint64, writeReq uint64) SSTReader {
	return SSTReader{
		file:         nil,
		SeqNm:        seqNum,
		partitionId:  0,
		startKey:     []byte(sKey),
		endKey:       []byte(eKey),
		noOfDelReq:   delReq,
		noOfWriteReq: writeReq,
	}
}

func TestOverlap(t *testing.T) {

	b1 := overlap("1", "5", "10", "6")
	b2 := overlap("1", "10", "12", "8")
	b3 := overlap("12", "8", "1", "10")
	b4 := overlap("4", "12", "9", "20")
	b5 := overlap("1", "12", "4", "7")
	b6 := overlap("200", "999", "222", "666")
	b7 := overlap("200", "250", "270", "300")

	fmt.Println(b1)
	fmt.Println(b2)
	fmt.Println(b3)
	fmt.Println(b4)
	fmt.Println(b5)
	fmt.Println(b6)
	fmt.Println(b7)
	/*require.False(t, b1, "should have been false")
	require.False(t, b2, "should have been false")
	require.False(t, b3, "should have been true")
	require.False(t, b4, "should have been true")
	require.False(t, b5, "should have been true")
	require.True(t, b6, "should have been true")
	require.False(t, b7, "should have been false")*/

	//TODO - write some true cases
}

func TestDisp(t *testing.T) {
	fmt.Println("11Key_" < "1Key_")
	fmt.Println("11Key_" > "1Key_")
	fmt.Println("11Key_" == "1Key_")
}

func TestBuildHeap(t *testing.T) {
	partitionId := 0
	dir, err := ioutil.TempDir("", "buildHeapTest")
	if err != nil {
		log.Fatal(err)
	}
	sst_file1, err := ioutil.TempFile(dir, fmt.Sprintf(SSTBaseFileName, partitionId, 0))
	sst_file2, err := ioutil.TempFile(dir, fmt.Sprintf(SSTBaseFileName, partitionId, 1))

	sstWriter1 := &SSTWriter{
		file:        sst_file1,
		writer:      bufio.NewWriter(sst_file1),
		partitionId: partitionId,
		SeqNum:      0,
	}
	sstWriter2 := &SSTWriter{
		file:        sst_file2,
		writer:      bufio.NewWriter(sst_file2),
		partitionId: partitionId,
		SeqNum:      1,
	}

	defer os.Remove(sst_file1.Name())
	defer os.Remove(sst_file2.Name())
	defer os.RemoveAll(dir)
	sKey_1 := []byte("")
	eKey_1 := []byte("")
	var writeCount_1 uint64 = 0
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("%dKey_", i)
		value := fmt.Sprintf("%dValue_", i)//TODO - this must be written in sorted order - V Imp

		t1 := time.Now().UnixNano()
		sstWriter1.Write([]byte(key), []byte(value), t1, defaultConstants.writeReq)
		writeCount_1++

		time.Sleep(time.Nanosecond * 10)
		t2 := time.Now().UnixNano()
		sstWriter1.Write([]byte(key), []byte(value), t2, defaultConstants.writeReq)
		writeCount_1++

		if i == 0 {
			sKey_1 = []byte(key)
		} else if i == 19 {
			eKey_1 = []byte(key)
		}

		fmt.Println(fmt.Sprintf("Key: %s, time: %d", key, t1))
		fmt.Println(fmt.Sprintf("Key: %s, time: %d", key, t2))
	}

	sKey_2 := []byte("")
	eKey_2 := []byte("")
	var writeCount_2 uint64 = 0
	var deleteCount_2 uint64 = 0
	for i := 10; i < 25; i++ {
		time.Sleep(time.Nanosecond * 10)
		key := fmt.Sprintf("%dKey_", i)
		value := fmt.Sprintf("%dValue_", i)
		sstWriter2.Write([]byte(key), []byte(value), time.Now().UnixNano(), defaultConstants.deleteReq)
		deleteCount_2++
		/*		if i%2 == 0 {
					sstWriter2.Write([]byte(key), []byte(value), time.Now().UnixNano()+1, defaultConstants.writeReq)
					writeCount_2++
				} else {
					sstWriter2.Write([]byte(key), []byte(value), time.Now().UnixNano()+1, defaultConstants.deleteReq)
					deleteCount_2++
				}
		*/
		if i == 10 {
			sKey_2 = []byte(key)
		} else if i == 24 {
			eKey_2 = []byte(key)
		}
	}

	sstWriter1.writer.Flush()
	sstWriter2.writer.Flush()
	sst_file1.Sync()
	sst_file2.Sync()

	sst_file1.Seek(0, 0)
	sst_file2.Seek(0, 0)
	compactInfo := initCompactInfo(0, partitionId)

	sReader1 := SSTReader{
		file:         sst_file1,
		SeqNm:        0,
		partitionId:  partitionId,
		startKey:     sKey_1,
		endKey:       eKey_1,
		noOfWriteReq: writeCount_1,
		noOfDelReq:   0,
	}
	sReader2 := SSTReader{
		file:         sst_file2,
		SeqNm:        1,
		partitionId:  partitionId,
		startKey:     sKey_2,
		endKey:       eKey_2,
		noOfWriteReq: writeCount_2,
		noOfDelReq:   deleteCount_2,
	}
	compactInfo.botLevelSST = append(compactInfo.botLevelSST, sReader1, sReader2)
	compactInfo.buildHeap()
	actualKeyOrder := make([]string, 0, 100)
	for compactInfo.heap.Len() > 0 {
		rec := compactInfo.nextRec()
		actualKeyOrder = append(actualKeyOrder, string(rec.key))
		//fmt.Println(fmt.Sprintf("Key: %s, Value: %s, RecType: %d, TS: %d",
		//	string(rec.key), string(rec.val), rec.meta.recType, rec.meta.ts))
	}
	expectedKeyOrder := [35]string{"0Key_", "10Key_", "11Key_", "12Key_", "13Key_", "14Key_", "15Key_", "16Key_", "17Key_", "18Key_", "19Key_", "1Key_", "20Key_", "21Key_", "22Key_", "23Key_", "24Key_", "2Key_", "3Key_", "4Key_", "5Key_", "6Key_", "7Key_", "8Key_", "9Key_", "10Key_", "11Key_", "12Key_", "13Key_", "14Key_", "15Key_", "16Key_", "17Key_", "18Key_", "19Key_"}
	for i, _ := range actualKeyOrder {
		require.Equal(t, expectedKeyOrder[i], actualKeyOrder[i], "keys(from compaction heap) must match")
	}
	for i := 0; i < len(actualKeyOrder)-1; i++ {
		if actualKeyOrder[i]> actualKeyOrder[i+1]{
			//panic("keys(from compaction heap) must be in sorted order")
			fmt.Println(actualKeyOrder[i],"  -  ",actualKeyOrder[i+1])
		}
	}
}
