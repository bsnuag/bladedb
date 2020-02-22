package bladedb

import (
	"bladedb/sklist"
	"container/heap"
	"fmt"
	"os"
	"sort"
)

type HeapEle struct {
	reader SSTReader
	rec    *SSTRec
}

type heapArr []*HeapEle

func (h heapArr) Len() int { return len(h) }
func (h heapArr) Less(i, j int) bool {
	if string(h[i].rec.key) == string(h[j].rec.key) { //rec with same key having max ts will be at top of heap - test it
		return h[i].rec.meta.ts > h[j].rec.meta.ts
	}
	return string(h[i].rec.key) < string(h[j].rec.key) //TODO - check if comparison works with string
}
func (h heapArr) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *heapArr) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(*HeapEle))
}

func (h *heapArr) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (h *heapArr) Top() interface{} { //get top , doesn't remove from heap
	return (*h)[0]
}

//TODO  - CompactInfo should get build and attached to partitionInfo to indicate current ongoing compaction

func (compactInfo *CompactInfo) buildHeap() {
	compactInfo.heap = make(heapArr, 0, len(compactInfo.botLevelSST)+len(compactInfo.topLevelSST))
	compactInfo.sortInputSST()
	//initialise heap with recs from all ssts
	for _, sstReader := range compactInfo.botLevelSST {
		_, sstRec := sstReader.readNext()
		if sstRec == nil { //shouldn't happen....each sst will have data
			fmt.Println(fmt.Sprintf("Found empty rec in sst file: %s", sstReader.file.Name()))
			continue
		}
		ele := &HeapEle{
			reader: sstReader,
			rec:    sstRec,
		}
		compactInfo.heap.Push(ele)
	}

	for _, sstReader := range compactInfo.topLevelSST {
		_, sstRec := sstReader.readNext()
		ele := &HeapEle{
			reader: sstReader,
			rec:    sstRec,
		}
		compactInfo.heap.Push(ele)
	}

	heap.Init(&compactInfo.heap)
}

type CompactInfo struct {
	partitionId   int
	thisLevel     int
	nextLevel     int //level to which sst will be compacted
	botLevelSST   []SSTReader
	topLevelSST   []SSTReader
	newSSTFileSeq []uint32
	idx           *sklist.SkipList
	heap          heapArr
}

//all SSTReader must have valid start-key and end-key
//sort sstFiles
func (compactInfo *CompactInfo) sortInputSST() {
	sort.Slice(compactInfo.topLevelSST, func(i, j int) bool {
		return string(compactInfo.topLevelSST[i].startKey) <= string(compactInfo.topLevelSST[i].startKey)
	})

	sort.Slice(compactInfo.botLevelSST, func(i, j int) bool {
		return string(compactInfo.botLevelSST[i].startKey) <= string(compactInfo.botLevelSST[i].startKey)
	})
}

var compactQueue = make(chan *CompactInfo, 50) // partId is communicated for compaction

//notification should be pushed only when
// 1. 0th level has enough SST - at least 4 files
// 2. compaction from 1st level to next should get trigger when a new file in 1st level gets created
//gets triggered when a new sst is created at level 0
func runCompactWorker() {
	for compactTask := range compactQueue {
		fmt.Println(fmt.Sprintf("received compaction request for partiton: %d, level: %d",
			compactTask.partitionId, compactTask.nextLevel))
		partitionInfo := partitionInfoMap[compactTask.partitionId]

		partitionInfo.compactLock.Lock() //TODO - check if we can use mutex inside channel
		var compactInfo *CompactInfo = nil
		var sKey, eKey = "", ""
		if partitionInfo.activeCompaction != nil {
			fmt.Println(fmt.Sprintf("Compaction is active for partition: %d, can not trigger another one",
				partitionInfo.partitionId))
			continue
		} else {
			compactInfo = initCompactInfo(compactTask.thisLevel, compactTask.partitionId)
			partitionInfo.activeCompaction = compactInfo
			fmt.Println(compactInfo)
			if compactInfo.thisLevel == 0 {
				sKey, eKey = compactInfo.fillLevels() //sKey, eKey  - range of query for which we are running compaction
			} else if compactInfo.thisLevel > 0 {
				sKey, eKey = compactInfo.fillTopLevels()
			}
			if sKey == "" && eKey == "" { //this condition will never arise
				fmt.Println(fmt.Sprintf("no overlapped ssts found for level: %d, partition: %d, ",
					compactTask.thisLevel, partitionInfo.partitionId))
				partitionInfo.activeCompaction = nil
				compactInfo = nil
			}
		}
		partitionInfo.compactLock.Unlock()

		if compactInfo != nil {
			compactInfo.compact()
			//TODO - all compacted files must be written to manifest file for deletion and newSST files should become active (loading to Index)
			//TODO - when new sst files are loaded int0 index and compacted files are removed - use read lock - if a thread is reading and file is delete -
			//update compacted SSTReaders
			partitionInfo.updatePartition()
			//TODO - check if nextLevel is eligible for compaction - then put it in the queue
			//TODO - complete compaction, make active compaction to nil
		}
	}
}

//TODO - post compaction 1.delete old SST files, 2. load new SST into index

//TODO - where compactInfo should get filled ?? when memtable gets flushed ?

func (compactInfo *CompactInfo) compact() {
	//if base level is > 0 and base level doesn't have overlapped ssts from top level then just update the level in manifest
	if compactInfo.thisLevel > 0 && len(compactInfo.topLevelSST) == 0 {
		compactInfo.updateLevel()
		return
	}
	err := compactInfo.updateSSTReader()//TODO - check if we can remove this
	if err != nil {
		panic(err)
	}

	pInfo := partitionInfoMap[compactInfo.partitionId]
	fmt.Println(pInfo)

	compactInfo.buildHeap()
	var bytesWritten uint32 = 0
	sstWriter, err := pInfo.NewSSTWriter() //all new sst will be in next level
	if err != nil {
		panic(err)
	}
	for compactInfo.heap.Len() > 0 {
		rec := compactInfo.nextRec()
		if compactInfo.nextLevel == defaultConstants.maxLevel && rec.meta.recType == defaultConstants.deleteReq {
			fmt.Println(fmt.Sprintf("Tombstone rec: %v, with target level=maxlevel, removing it from adding it "+
				"to compacted sst", rec))
			continue
		}
		indexRec := &IndexRec{
			SSTRecOffset:  sstWriter.Offset,
			SSTFileSeqNum: sstWriter.SeqNum,
			SSTFileLevel:  compactInfo.nextLevel,
			TS:            rec.meta.ts,
		}
		n, err := sstWriter.Write(rec.key, rec.val, rec.meta.ts, rec.meta.recType)
		//TODO - can we build temp index with SST and later merge with active index...will increase mem footprint but will reduce re-load time of SST to index
		if err != nil {
			fmt.Println(fmt.Sprintf("Error while writing SST during compaction, sstFile: %s, err: %v",
				sstWriter.file.Name(), err)) //TODO - what to do here ? abort compaction ..?
		}
		bytesWritten += uint32(n)

		if bytesWritten >= defaultConstants.maxSSTSize {
			seqNum, err := sstWriter.FlushAndClose()
			if err != nil {
				panic(err)
			}
			compactInfo.newSSTFileSeq = append(compactInfo.newSSTFileSeq, seqNum)
			sstWriter, _ = pInfo.NewSSTWriter() //all new sst will be in next level
			bytesWritten = 0
		}
		keyHash, _ := GetHash(rec.key)
		compactInfo.idx.Set(keyHash, indexRec)
	}

	fmt.Println("Compaction got completed..tmpIndex size: ", compactInfo.idx.Length)
	seqNum, err := sstWriter.FlushAndClose()
	if err != nil {
		panic(err)
	}

	//if nothing written, delete the file
	if bytesWritten == 0 {
		err := deleteSST(pInfo.partitionId, seqNum)
		if err != nil {
			panic(err)
		}
	} else {
		compactInfo.newSSTFileSeq = append(compactInfo.newSSTFileSeq, seqNum)
	}
}

func (pInfo *PartitionInfo) updatePartition() {
	manifestRecs := make([]ManifestRec, 0, 8)
	pInfo.levelLock.Lock()
	compactInfo := pInfo.activeCompaction
	thisLevelInfo := pInfo.levelsInfo[compactInfo.thisLevel]
	nextLevelInfo := pInfo.levelsInfo[compactInfo.nextLevel]

	//update active sstReaderMap with newly created ssts
	for _, sstSeq := range compactInfo.newSSTFileSeq {
		mf1 := ManifestRec{
			partitionId: pInfo.partitionId,
			seqNum:      sstSeq,
			levelNum:    compactInfo.nextLevel,
			fop:         defaultConstants.fileCreate,
			fileType:    defaultConstants.sstFileType,
		}
		manifestRecs = append(manifestRecs, mf1)
		sstReader, err := NewSSTReader(sstSeq, pInfo.partitionId)
		if err != nil {
			panic(err)
		}
		nextLevelInfo.SSTReaderMap[sstReader.SeqNm] = sstReader
	}
	pInfo.levelLock.Unlock()

	//update active index with new index data (new ssts)
	iterator := compactInfo.idx.NewIterator()
	for iterator.Next() {
		tmpKeyHash := iterator.Value().Key()
		tmpIndexRec := iterator.Value().Value().(*IndexRec)

		indexVal := pInfo.index.Get(tmpKeyHash)
		if indexVal == nil {
			pInfo.index.Set(tmpKeyHash, tmpIndexRec)
		} else {
			idxRec := indexVal.Value().(*IndexRec)
			if idxRec.TS > tmpIndexRec.TS { //if there is a latest write
				continue
			}
			pInfo.index.Set(tmpKeyHash, tmpIndexRec)
		}
	}

	deleteReaders := make([]SSTReader, 0, 8)
	//remove compacted sst's from active sstReaderMap
	pInfo.levelLock.Lock()
	for _, reader := range compactInfo.botLevelSST {
		sstReader := thisLevelInfo.SSTReaderMap[reader.SeqNm]
		sstReader.Close()
		delete(thisLevelInfo.SSTReaderMap, reader.SeqNm)
		deleteReaders = append(deleteReaders, reader)
		mf1 := ManifestRec{
			partitionId: pInfo.partitionId,
			seqNum:      reader.SeqNm,
			levelNum:    compactInfo.thisLevel,
			fop:         defaultConstants.fileDelete,
			fileType:    defaultConstants.sstFileType,
		}
		manifestRecs = append(manifestRecs, mf1)
	}

	for _, reader := range compactInfo.topLevelSST {
		sstReader := nextLevelInfo.SSTReaderMap[reader.SeqNm]
		sstReader.Close()
		delete(nextLevelInfo.SSTReaderMap, reader.SeqNm)
		deleteReaders = append(deleteReaders, reader)
		mf1 := ManifestRec{
			partitionId: pInfo.partitionId,
			seqNum:      reader.SeqNm,
			levelNum:    compactInfo.nextLevel,
			fop:         defaultConstants.fileDelete,
			fileType:    defaultConstants.sstFileType,
		}
		manifestRecs = append(manifestRecs, mf1)
	}
	pInfo.levelLock.Unlock()
	write(manifestRecs) //TODO - check for consistent - can we name sst and temp and post manifest write confirm it to permanent
	for _, reader := range deleteReaders {
		reader.closeAndDelete()
	}
}

func (compactInfo *CompactInfo) nextRec() *SSTRec {
	if compactInfo.heap.Len() <= 0 {
		return nil
	}

	minPop := heap.Pop(&compactInfo.heap).(*HeapEle)
	compactInfo.pushNext(minPop)

	for compactInfo.heap.Len() > 0 {
		//if top is a diff element - break the loop
		if string(compactInfo.heap.Top().(*HeapEle).rec.key) != string(minPop.rec.key) {
			break
		}
		pop := heap.Pop(&compactInfo.heap).(*HeapEle)

		//if string(minPop.rec.key) != string(pop.rec.key) {
		//	compactInfo.heap.Push(pop)
		//	break
		//}

		if string(minPop.rec.key) == string(pop.rec.key) {
			if pop.rec.meta.ts > minPop.rec.meta.ts { //TODO - THIS condition will always be true - check and remove it
				minPop = pop
			}
			compactInfo.pushNext(pop)
		}
	}

	return minPop.rec
}

func (compactInfo *CompactInfo) pushNext(popEle *HeapEle) bool {
	_, nextRec := popEle.reader.readNext()

	if nextRec == nil { //TODO - We can later use this info to delete this file once compaction process is done + new sst is loaded to index
		fmt.Println(fmt.Sprintf("reached end of file: %s, closing reader", popEle.reader.file.Name()))
		popEle.reader.Close()
		return false
	}

	ele := &HeapEle{
		reader: popEle.reader,
		rec:    nextRec,
	}
	heap.Push(&compactInfo.heap, ele)
	return true
}

//when current level is 0 and next level is 1
func (compactInfo *CompactInfo) fillLevels() (string, string) {
	pInfo := partitionInfoMap[compactInfo.partitionId]

	//fmt.Println(pInfo)
	level := compactInfo.thisLevel

	pInfo.levelLock.RLock()
	thisLevelSSTs := pInfo.levelsInfo[level].SSTReaderMap
	nextLevelSSTs := pInfo.levelsInfo[compactInfo.nextLevel].SSTReaderMap
	thisLevelSortedSSTKeys := sortedSST(thisLevelSSTs)
	pInfo.levelLock.RUnlock()

	//compactHelper := make([]CompactHelper, len(thisLevelSSTs.SSTReaderMap))

	//sorted ssts seq_num of thisLevel

	thisLevel_sKey := ""
	thisLevel_eKey := ""

	//collect all sst's from this level until total overlapped compact files are less than maxSSTCompact
	//fist file of this level gets added automatically irrespective of number of match with next level
	nextLevelOverlappedSSTMap := make(map[uint32]SSTReader)
	for _, sstSeq := range thisLevelSortedSSTKeys {
		sstReader := thisLevelSSTs[uint32(sstSeq)]
		sKey := string(sstReader.startKey)
		eKey := string(sstReader.endKey)
		overlappedSST := overlappedSSTs(nextLevelSSTs, sKey, eKey)
		fmt.Println("overlappedSST- ", overlappedSST, " for key range- ", sKey, eKey)
		if len(overlappedSST) != 0 {
			if len(compactInfo.botLevelSST) == 0 { //first file must get included irrespective of overlap count
				fmt.Println(sstReader)
				compactInfo.botLevelSST = append(compactInfo.botLevelSST, sstReader)
				fillOverlappedMap(nextLevelOverlappedSSTMap, overlappedSST)
				thisLevel_sKey = sKey
				thisLevel_eKey = eKey
			} else {
				if len(overlappedSST) > defaultConstants.maxSSTCompact {
					continue
				}
				tmp_sKey, tmp_eKey := keyRange(sKey, eKey, thisLevel_sKey, thisLevel_eKey)
				fmt.Println("tmp_sKey, tmp_eKey :", tmp_sKey, tmp_eKey)
				overlappedSST := overlappedSSTs(nextLevelSSTs, tmp_sKey, tmp_eKey)
				if len(overlappedSST) > 0 && len(overlappedSST) <= defaultConstants.maxSSTCompact {
					fmt.Println(sstReader)
					compactInfo.botLevelSST = append(compactInfo.botLevelSST, sstReader)
					fillOverlappedMap(nextLevelOverlappedSSTMap, overlappedSST)
					thisLevel_sKey = tmp_sKey
					thisLevel_eKey = tmp_eKey
				}
			}
		}
	}

	//if no overlapped found for individual level 0 files, take all bot files and do compact
	if len(compactInfo.botLevelSST) == 0 {
		for _, reader := range thisLevelSSTs {
			fmt.Println(reader.SeqNm, " **")
			compactInfo.botLevelSST = append(compactInfo.botLevelSST, reader)
			thisLevel_sKey, thisLevel_eKey = keyRange(string(reader.startKey),
				string(reader.endKey), thisLevel_sKey, thisLevel_eKey)
		}
	}

	overlappedSST := overlappedSSTs(nextLevelSSTs, thisLevel_sKey, thisLevel_eKey)
	if len(overlappedSST) > 0 {
		fillOverlappedMap(nextLevelOverlappedSSTMap, overlappedSST)
	}

	for _, sstReader := range nextLevelOverlappedSSTMap {
		compactInfo.topLevelSST = append(compactInfo.topLevelSST, sstReader)
	}
	//New SST file must be _temp..once we complete write for the new SSTs we must rename this file (removing _temp) + load index and write delete meta in manifesto
	return thisLevel_sKey, thisLevel_eKey
}

func fillOverlappedMap(overlappedMap map[uint32]SSTReader, readers []SSTReader) {
	for _, reader := range readers {
		overlappedMap[reader.SeqNm] = reader
	}
}

func (compactInfo *CompactInfo) fillTopLevels() (string, string) {
//for bot level > 0, pick ssts if count is more than levelMaxSST
	pInfo := partitionInfoMap[compactInfo.partitionId]

	level := compactInfo.thisLevel
	levelMax := defaultConstants.levelMaxSST[level]

	pInfo.levelLock.RLock()
	thisLevelSSTs := pInfo.levelsInfo[level].SSTReaderMap
	nextLevelSSTs := pInfo.levelsInfo[compactInfo.nextLevel].SSTReaderMap
	thisLevelSortedSSTKeys := sortedSST(thisLevelSSTs)
	pInfo.levelLock.RUnlock()

	thisLevel_sKey := ""
	thisLevel_eKey := ""

	var count uint32 = 0
	//nextLevelOverlappedSSTMap := make(map[uint32]SSTReader)
	for _, sstSeq := range thisLevelSortedSSTKeys {
		if uint32(len(thisLevelSSTs))-count < levelMax {
			break
		}
		sstReader := thisLevelSSTs[uint32(sstSeq)]
		compactInfo.botLevelSST = append(compactInfo.botLevelSST, sstReader)
		sKey, eKey := string(sstReader.startKey), string(sstReader.endKey)
		thisLevel_sKey, thisLevel_eKey = keyRange(sKey, eKey, thisLevel_sKey, thisLevel_eKey)
		//overlappedSST := overlappedSSTs(nextLevelSSTs, sKey, eKey)
		//fillOverlappedMap(nextLevelOverlappedSSTMap, overlappedSST)
		//if len(overlappedSST) > 0 {
		//	thisLevel_sKey, thisLevel_eKey = keyRange(sKey, eKey, thisLevel_sKey, thisLevel_eKey)
		//}
		count++
	}
	overlappedSST := overlappedSSTs(nextLevelSSTs, thisLevel_sKey, thisLevel_eKey)
	compactInfo.topLevelSST = overlappedSST
	//for _, sstReader := range nextLevelOverlappedSSTMap {
	//	compactInfo.topLevelSST = append(compactInfo.topLevelSST, sstReader)
	//}
	return thisLevel_sKey, thisLevel_eKey
}

func keyRange(sKey, eKey, final_sKey, final_eKey string) (string, string) {
	if sKey < final_sKey || final_sKey == "" {
		final_sKey = sKey
	}
	if eKey > final_eKey || final_eKey == "" {
		final_eKey = eKey
	}
	return final_sKey, final_eKey
}

func sortedSST(sstReaders map[uint32]SSTReader) []uint32 {
	//fmt.Println(sstReaders)
	sortedKey := make([]uint32, 0, len(sstReaders))
	for k := range sstReaders {
		sortedKey = append(sortedKey, k)
	}
	fmt.Println(sortedKey)
	//1. sort based on number of delete request in desc order
	//2. if del req are equal sort based on sst seq num in asc order
	sort.Slice(sortedKey, func(i, j int) bool { //TODO  - check divisible by zero error
		fmt.Println(sortedKey[i])
		delToWrite_i := computeDeltoWriteRatio(sstReaders[sortedKey[i]].noOfDelReq, sstReaders[sortedKey[i]].noOfWriteReq)
		delToWrite_j := computeDeltoWriteRatio(sstReaders[sortedKey[j]].noOfDelReq, sstReaders[sortedKey[j]].noOfWriteReq)
		if delToWrite_i > delToWrite_j {
			return true
		}
		return sstReaders[sortedKey[i]].SeqNm < sstReaders[sortedKey[j]].SeqNm
	})
	return sortedKey
}

func computeDeltoWriteRatio(deleteCount uint64, writeCount uint64) int {
	return (int)(deleteCount / (deleteCount + writeCount))
}

func overlappedSSTs(readers map[uint32]SSTReader, sKey string, eKey string) []SSTReader {
	overlappedSST := make([]SSTReader, 0, 10)
	for _, sstReader := range readers {
		if overlap(sKey, eKey, string(sstReader.startKey), string(sstReader.endKey)) {
			overlappedSST = append(overlappedSST, sstReader)
		}
	}
	return overlappedSST
}

func overlap(sKey1, eKey1, sKey2, eKey2 string) bool {
	if sKey1 >= sKey2 && sKey1 <= eKey2 {
		return true
	}
	if sKey2 >= sKey1 && sKey2 <= eKey1 {
		return true
	}
	return false
}

func (compactInfo *CompactInfo) updateSSTReader() error {
	for _, reader := range compactInfo.botLevelSST {
		file, err := os.OpenFile(reader.file.Name(), os.O_RDONLY, 0644)
		if err != nil {
			panic("Error while opening sst file, fileName: " + reader.file.Name())
			return err
		}
		reader.file = file
	}

	for _, reader := range compactInfo.topLevelSST {
		file, err := os.OpenFile(reader.file.Name(), os.O_RDONLY, 0644)
		if err != nil {
			panic("Error while opening sst file, fileName: " + reader.file.Name())
			return err
		}
		reader.file = file
	}
	return nil
}

func (compactInfo *CompactInfo) updateLevel() {
	pInfo := partitionInfoMap[compactInfo.partitionId]
	for _, reader := range compactInfo.botLevelSST {
		pInfo.levelLock.Lock()

		delete(pInfo.levelsInfo[compactInfo.thisLevel].SSTReaderMap, reader.SeqNm)
		pInfo.levelsInfo[compactInfo.nextLevel].SSTReaderMap[reader.SeqNm] = reader
		mf1 := ManifestRec{
			partitionId: pInfo.partitionId,
			levelNum:    compactInfo.nextLevel,
			seqNum:      reader.SeqNm,
			fop:         defaultConstants.fileCreate,
			fileType:    defaultConstants.sstFileType,
		}
		write([]ManifestRec{mf1})
		pInfo.levelLock.Unlock()
	}
}

func initCompactInfo(level int, partId int) *CompactInfo {
	return &CompactInfo{
		partitionId:   partId,
		thisLevel:     level,
		nextLevel:     level + 1,
		botLevelSST:   make([]SSTReader, 0, 8),
		topLevelSST:   make([]SSTReader, 0, 8),
		newSSTFileSeq: make([]uint32, 0, 100),
		idx:           sklist.New(),
		heap:          make([]*HeapEle, 0, 10000),
	}
}
