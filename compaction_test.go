package bladedb

import (
	"bladedb/memstore"
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
	pInfo, _ := NewPartition(partId)

	lInfo0 := pInfo.levelsInfo[0]
	lInfo0.sstSeqNums[123] = struct{}{}
	lInfo0.sstSeqNums[124] = struct{}{}
	lInfo0.sstSeqNums[125] = struct{}{}

	lInfo1 := pInfo.levelsInfo[1]
	lInfo1.sstSeqNums[234] = struct{}{}

	pInfo.sstReaderMap[123] = tempSSTReader("200", "250", 123, 0, 12)
	pInfo.sstReaderMap[124] = tempSSTReader("270", "300", 124, 0, 14)
	pInfo.sstReaderMap[125] = tempSSTReader("998", "999", 125, 0, 101)

	pInfo.sstReaderMap[234] = tempSSTReader("222", "666", 234, 0, 100)

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

	pInfo, _ := NewPartition(partId)

	pInfo.levelsInfo[0].sstSeqNums[123] = struct{}{}
	pInfo.levelsInfo[0].sstSeqNums[124] = struct{}{}
	pInfo.levelsInfo[0].sstSeqNums[125] = struct{}{}

	pInfo.sstReaderMap[123] = tempSSTReader("200", "250", 123, 0, 12)
	pInfo.sstReaderMap[124] = tempSSTReader("270", "300", 124, 0, 14)
	pInfo.sstReaderMap[125] = tempSSTReader("998", "999", 125, 0, 101)

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
	pInfo, _ := NewPartition(partId)

	//Level 1 have only 3 sst which is lower than levelMaxSST[1] - this should be taken care when compactionInfo is being pushed to compactQueue
	pInfo.levelsInfo[1].sstSeqNums[123] = struct{}{}
	pInfo.levelsInfo[1].sstSeqNums[124] = struct{}{}
	pInfo.levelsInfo[1].sstSeqNums[125] = struct{}{}

	pInfo.sstReaderMap[123] = tempSSTReader("200", "250", 123, 7, 12)
	pInfo.sstReaderMap[124] = tempSSTReader("270", "300", 124, 0, 14)
	pInfo.sstReaderMap[125] = tempSSTReader("998", "999", 125, 40, 101)

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
	require.Equal(t, 0, compactInfo.idx.Size(), "level 1 has only 3 files i.e. less than levelMaxSST, index length should be zero")
}

func TestFillLevels_level1WithOverlap(t *testing.T) {
	partId := 0
	level := 1
	DefaultConstants.levelMaxSST[level] = 2
	pInfo, _ := NewPartition(partId)

	pInfo.levelsInfo[1].sstSeqNums[123] = struct{}{}
	pInfo.levelsInfo[1].sstSeqNums[124] = struct{}{}
	pInfo.levelsInfo[1].sstSeqNums[125] = struct{}{}

	pInfo.sstReaderMap[123] = tempSSTReader("200", "250", 123, 0, 12)
	pInfo.sstReaderMap[124] = tempSSTReader("270", "300", 124, 0, 14)
	pInfo.sstReaderMap[125] = tempSSTReader("998", "999", 125, 0, 101)

	pInfo.levelsInfo[2].sstSeqNums[234] = struct{}{}
	pInfo.sstReaderMap[234] = tempSSTReader("222", "666", 234, 0, 100)

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
	DefaultConstants.levelMaxSST[level] = 1
	pInfo, _ := NewPartition(partId)

	pInfo.levelsInfo[1].sstSeqNums[123] = struct{}{}
	pInfo.levelsInfo[1].sstSeqNums[124] = struct{}{}
	pInfo.levelsInfo[1].sstSeqNums[125] = struct{}{}

	pInfo.sstReaderMap[123] = tempSSTReader("200", "250", 123, 0, 12)
	pInfo.sstReaderMap[124] = tempSSTReader("270", "300", 124, 0, 14)
	pInfo.sstReaderMap[125] = tempSSTReader("998", "999", 125, 0, 101)

	pInfo.levelsInfo[2].sstSeqNums[234] = struct{}{}
	pInfo.sstReaderMap[234] = tempSSTReader("000", "199", 234, 0, 100)

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

func TestBuildCompactionBaseLevelAs1(t *testing.T) {
	partitionId := 0
	dir, err := ioutil.TempDir("", "compactTest")
	if err != nil {
		log.Fatal(err)
	}
	//initiate manifestFile
	mFile, err := ioutil.TempFile(dir, "MANIFEST.*.txt")
	manifestFile = &ManifestFile{
		file: mFile,
	}
	// update SSTDir to temp directory
	SSTDir = dir
	partitionInfoMap[partitionId], _ = NewPartition(partitionId)
	partitionInfoMap[partitionId].sstSeq = 100
	partitionInfoMap[partitionId].walSeq = 0

	compactInfo := initCompactInfo(1, partitionId)
	sReader1, sReader2 := prepareInputSSTs(dir, partitionId)

	defer os.Remove(mFile.Name())
	defer os.Remove(sReader1.file.Name())
	defer os.Remove(sReader2.file.Name())
	defer os.RemoveAll(dir)

	compactInfo.botLevelSST = append(compactInfo.botLevelSST, sReader1, sReader2)
	//update bottom level reader map
	partitionInfoMap[partitionId].levelsInfo[1].sstSeqNums[sReader1.SeqNm] = struct{}{}
	partitionInfoMap[partitionId].levelsInfo[1].sstSeqNums[sReader2.SeqNm] = struct{}{}

	partitionInfoMap[partitionId].sstReaderMap[sReader1.SeqNm] = sReader1
	partitionInfoMap[partitionId].sstReaderMap[sReader2.SeqNm] = sReader2

	compactInfo.compact()
	replay() //manifest replay

	require.Equal(t, 0, len(partitionInfoMap[partitionId].levelsInfo[1].sstSeqNums),
		"expecting no(zero) ssts in level 1 post compaction")

	require.Equal(t, 2, len(partitionInfoMap[partitionId].levelsInfo[2].sstSeqNums),
		"expecting 2 ssts in level 2 post compaction")

	require.Equal(t, 0, len(manifestFile.manifest.logManifest[partitionId].manifestRecs),
		"logManifest length should be zero")

	require.Equal(t, 0, len(compactInfo.newSSTReaders), "expecting no new ssts post compaction")
	for _, mRec := range manifestFile.manifest.sstManifest[partitionId].manifestRecs {
		require.Equal(t, 2, mRec.levelNum, "level upgrade should happen if no overlap in top level")
	}
}

func TestBuildCompactionBaseLevelAs0(t *testing.T) {
	partitionId := 0
	dir, err := ioutil.TempDir("", "buildHeapTest")
	if err != nil {
		log.Fatal(err)
	}
	SSTDir = dir
	partitionInfoMap[partitionId], _ = NewPartition(partitionId)
	partitionInfoMap[partitionId].sstSeq = 100
	partitionInfoMap[partitionId].walSeq = 0

	compactInfo := initCompactInfo(0, partitionId)
	sReader1, sReader2 := prepareInputSSTs(dir, partitionId)
	compactInfo.botLevelSST = append(compactInfo.botLevelSST, sReader1, sReader2)

	defer os.Remove(sReader1.file.Name())
	defer os.Remove(sReader2.file.Name())
	defer os.RemoveAll(dir)

	compactInfo.compact()
	require.Equal(t, 0, compactInfo.heap.Len(), "Heap size should be zero post compaction")
	require.Equal(t, 1, len(compactInfo.newSSTReaders), "Expecting only one new SST file post compaction")

	actualKeyOrder := make([]string, 0, 100)
	sstRecCount := 0
	reader := compactInfo.newSSTReaders[0]
	iterator := reader.NewIterator()
	for {
		if rec, ok := iterator.Next(); ok {
			actualKeyOrder = append(actualKeyOrder, string(rec.key))
			sstRecCount++
			continue
		}
		break
	}
	for i := 0; i < len(actualKeyOrder)-1; i++ {
		require.LessOrEqual(t, actualKeyOrder[i], actualKeyOrder[i+1], "keys(from compaction heap) must be in sorted order")
	}
	require.Equal(t, 10, compactInfo.idx.Size(), "Expecting 10 entries (non-deleted) in index post compaction")
	require.Equal(t, 25, sstRecCount, "Expecting 25 recs in new sst post compaction")
}

func prepareInputSSTs(dir string, partitionId int) (SSTReader, SSTReader) {
	SSTDir = dir
	sstWriter1, _ := NewSSTWriter(partitionId, 0)
	sstWriter2, _ := NewSSTWriter(partitionId, 1)
	//Write data into mem and then flush it to sst
	sKe1 := ""
	eKey1 := ""
	var writeCount1 uint64 = 0
	memTable, _ := memstore.NewMemStore()
	for i := 0; i < 20; i++ {
		time.Sleep(time.Nanosecond * 10)
		key, value := fmt.Sprintf("%dKey_", i), fmt.Sprintf("%dValue_", i)
		memTable.Insert([]byte(key), []byte(value), NanoTime(), DefaultConstants.writeReq)
	}
	var sstEncoderBuf = make([]byte, uint32(sstBufLen))
	iterator := memTable.Recs().NewIterator()
	for iterator.Next() {
		next := iterator.Value()
		key := []byte(next.Key())
		mRec := next.Value().(*memstore.MemRec)
		sstRec := SSTRec{mRec.RecType, key, mRec.Value, mRec.TS}
		n := sstRec.SSTEncoder(sstEncoderBuf[:])
		sstWriter1.Write(sstEncoderBuf[:n])
		writeCount1++
		if sKe1 == "" {
			sKe1 = string(key)
		}
		eKey1 = string(key)
		//fmt.Println(fmt.Sprintf("Key: %s, time: %d", string(mRec.Key), mRec.TS))
		iterator.Next()
	}

	//Write data into mem and then flush it to sst
	sKey2 := ""
	eKey2 := ""
	memTable.RefreshMemTable()
	var deleteCount2 uint64 = 0
	for i := 10; i < 25; i++ {
		time.Sleep(time.Nanosecond * 10)
		key, value := fmt.Sprintf("%dKey_", i), fmt.Sprintf("%dValue_", i)
		memTable.Insert([]byte(key), []byte(value), NanoTime(), DefaultConstants.deleteReq)
	}
	iterator = memTable.Recs().NewIterator()
	for iterator.Next() {
		next := iterator.Value()
		key := []byte(next.Key())
		mRec := next.Value().(*memstore.MemRec)
		sstRec := SSTRec{mRec.RecType, key, mRec.Value, mRec.TS}
		n := sstRec.SSTEncoder(sstEncoderBuf[:])
		sstWriter2.Write(sstEncoderBuf[:n])
		deleteCount2++
		if sKey2 == "" {
			sKey2 = string(key)
		}
		eKey2 = string(key)
		//fmt.Println(fmt.Sprintf("Key: %s, time: %d", string(mRec.Key), mRec.TS))
		iterator.Next()
	}
	sstWriter1.FlushAndClose()
	sstWriter2.FlushAndClose()

	sReader1, _ := NewSSTReader(sstWriter1.SeqNum, sstWriter1.partitionId)
	sReader1.startKey = []byte(sKe1)
	sReader1.endKey = []byte(eKey1)
	sReader1.noOfWriteReq = writeCount1
	sReader1.noOfDelReq = 0

	sReader2, _ := NewSSTReader(sstWriter2.SeqNum, sstWriter2.partitionId)
	sReader2.startKey = []byte(sKey2)
	sReader2.endKey = []byte(eKey2)
	sReader2.noOfWriteReq = 0
	sReader2.noOfDelReq = deleteCount2
	return sReader1, sReader2
}

func TestStopCompactWorker(t *testing.T) {
	DefaultConstants.compactWorker = 0
	stopCompactWorker()
	require.True(t, compactActive == 0)
	require.False(t, isCompactionActive())
	require.Nil(t, compactTaskQueue)

}

func TestStopCompactWorker_WithPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("The code did not panic")
		}
	}()
	DefaultConstants.compactWorker = 1
	stopCompactWorker()
	require.True(t, compactActive == 0)
	require.Nil(t, compactTaskQueue)
}
