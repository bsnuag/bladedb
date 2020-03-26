package bladedb

import (
	"bladedb/index"
	"container/heap"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type HeapEle struct {
	reader SSTReader
	rec    *SSTRec
}

type heapArr []*HeapEle

func (h heapArr) Len() int { return len(h) }
func (h heapArr) Less(i, j int) bool {
	if string(h[i].rec.key) == string(h[j].rec.key) { //rec with same key having max ts will be at top of heap
		return h[i].rec.meta.ts > h[j].rec.meta.ts
	}
	return string(h[i].rec.key) < string(h[j].rec.key)
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
	newSSTReaders []SSTReader
	idx           *index.SkipList
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

var compactTaskQueue = make(chan *CompactInfo, 10000) //TODO - change channel type to compactTask with pId and thisLevel
var compactSubscriber sync.WaitGroup
var compactActive = DefaultConstants.compactActive //replace with atomic operation

//publish new task to compact queue if compaction is active(compactActive==true)
func publishCompactTask(compactTask *CompactInfo) {
	if isCompactionActive() {
		compactTaskQueue <- compactTask
	} else {
		fmt.Println("Compaction is not active, cannot publish new task")
	}
}

func isCompactionActive() bool {
	return compactActive == 1
}

//blocks from accepting new compact tasks, but waits to complete already submitted tasks
func stopCompactWorker() {
	s := time.Now()
	fmt.Println("Request received to stop compact workers")
	atomic.AddInt32(&compactActive, -1)
	close(compactTaskQueue)
	fmt.Println("No new compaction task would be taken, already published tasks will be completed")

	fmt.Println("Waiting for all submitted compaction tasks to be completed")
	compactSubscriber.Wait()
	fmt.Println("all submitted compaction tasks completed, time taken(Sec): ", time.Since(s).Seconds())
}

func activateCompactWorkers() {
	for i := 1; i <= DefaultConstants.compactWorker; i++ {
		go compactWorker(fmt.Sprintf("CompactWorker- %d", i))
	}
}

//notification should be pushed only when
// 1. 0th level has enough SST - at least 4 files
// 2. compaction from 1st level to next should get trigger when a new file in 1st level gets created
//gets triggered when a new sst is created at level 0
func compactWorker(workerName string) {
	compactSubscriber.Add(1)
	defer compactSubscriber.Done()
	for {
		compactTask, ok := <-compactTaskQueue
		if !ok {
			fmt.Println(fmt.Sprintf("Received signal to stop compact worker, exiting: %s", workerName))
			break
		}
		fmt.Println(fmt.Sprintf("received compaction request for partiton: %d, level: %d",
			compactTask.partitionId, compactTask.nextLevel))
		partitionInfo := partitionInfoMap[compactTask.partitionId]

		partitionInfo.compactLock.Lock() //TODO - check if using mutex inside channel a good practice
		var compactInfo *CompactInfo = nil
		var sKey, eKey = "", ""
		if partitionInfo.activeCompaction != nil {
			fmt.Println(fmt.Sprintf("Compaction is active for partition: %d, can not trigger another one",
				partitionInfo.partitionId))
		} else {
			compactInfo = initCompactInfo(compactTask.thisLevel, compactTask.partitionId)
			partitionInfo.activeCompaction = compactInfo
			fmt.Println(compactInfo)
			if compactInfo.thisLevel == 0 {
				sKey, eKey = compactInfo.fillLevels() //sKey, eKey  - range of query for which we are running compaction
			} else if compactInfo.thisLevel > 0 {
				sKey, eKey = compactInfo.fillTopLevels()
			}
			if sKey == "" && eKey == "" { //this condition will never arise - check it
				fmt.Println(fmt.Sprintf("no overlapped ssts found for level: %d, partition: %d, ",
					compactTask.thisLevel, partitionInfo.partitionId))
				partitionInfo.activeCompaction = nil
				compactInfo = nil
			}
		}
		partitionInfo.compactLock.Unlock()

		if compactInfo != nil {
			compactStartTime := time.Now()
			compactInfo.compact()
			if len(compactInfo.newSSTReaders) > 0 {
				partitionInfo.updatePartition()
			}
			//check if nextLevel is eligible for compaction
			partitionInfo.checkAndCompleteCompaction(compactInfo.nextLevel)
			fmt.Println(fmt.Sprintf("Compaction completed....."+
				"Compaction Stats-\n "+
				"Total execution time(sec): %f\n "+
				"Total Bottom Level Files: %d\n "+
				"Total Top Level Files: %d\n "+
				"Compacted Index Size:%d\n "+
				"New SSTs generated:%v\n",
				time.Since(compactStartTime).Seconds(), len(compactInfo.botLevelSST),
				len(compactInfo.topLevelSST), compactInfo.idx.Length, compactInfo.newSSTReaders))
		}
	}
}

func (pInfo *PartitionInfo) checkAndCompleteCompaction(level int) {
	pInfo.compactLock.Lock()
	pInfo.activeCompaction = nil //complete compaction
	pInfo.compactLock.Unlock()

	if level != DefaultConstants.maxLevel && len(pInfo.levelsInfo[level].sstSeqNums) > int(DefaultConstants.levelMaxSST[level]) {
		publishCompactTask(&CompactInfo{
			partitionId: pInfo.partitionId,
			thisLevel:   level,
		})
	}
}

func (compactInfo *CompactInfo) compact() {
	//if base level is > 0 and base level doesn't have overlapped ssts from top level then just update the level in manifest
	if compactInfo.thisLevel > 0 && len(compactInfo.topLevelSST) == 0 {
		compactInfo.updateLevel()
		return
	}
	err := compactInfo.updateSSTReader() //TODO - check if we can remove this
	if err != nil {
		panic(err)
	}

	pInfo := partitionInfoMap[compactInfo.partitionId]
	fmt.Println(pInfo)

	compactInfo.buildHeap()
	var bytesWritten uint32 = 0
	sstWriter, err := pInfo.NewSSTWriter()
	if err != nil {
		panic(err)
	}
	totalCompactIndexWrite := 0
	for compactInfo.heap.Len() > 0 {
		rec := compactInfo.nextRec()
		if compactInfo.nextLevel == DefaultConstants.maxLevel && rec.meta.recType == DefaultConstants.deleteReq {
			fmt.Println(fmt.Sprintf("Tombstone rec: %v, with target level=maxlevel, removing it from adding it "+
				"to compacted sst", rec))
			continue
		}
		indexRec := index.IndexRec{
			SSTRecOffset:  sstWriter.Offset,
			SSTFileSeqNum: sstWriter.SeqNum,
			TS:            rec.meta.ts,
		}
		totalCompactIndexWrite++
		n, err := sstWriter.Write(rec.key, rec.val, rec.meta.ts, rec.meta.recType)
		if err != nil {
			fmt.Println(fmt.Sprintf("Error while writing SST during compaction, sstFile: %s, err: %v",
				sstWriter.file.Name(), err)) //TODO - what to do here ? abort compaction ..?
		}
		bytesWritten += uint32(n)

		if rec.meta.recType == DefaultConstants.deleteReq {
			sstWriter.noOfDelReq++
		} else if rec.meta.recType == DefaultConstants.writeReq {
			sstWriter.noOfWriteReq++
		}
		if sstWriter.startKey == nil {
			sstWriter.startKey = rec.key
		}
		sstWriter.endKey = rec.key
		if bytesWritten >= DefaultConstants.maxSSTSize {
			seqNum, err := sstWriter.FlushAndClose()
			if err != nil {
				panic(err)
			}
			reader, err := NewSSTReader(seqNum, compactInfo.partitionId)
			if err != nil {
				panic(err)
			}
			reader.startKey = sstWriter.startKey
			reader.endKey = sstWriter.endKey
			reader.noOfWriteReq = sstWriter.noOfWriteReq
			reader.noOfDelReq = sstWriter.noOfDelReq
			compactInfo.newSSTReaders = append(compactInfo.newSSTReaders, reader)
			sstWriter, _ = pInfo.NewSSTWriter() //all new sst will be in next level
			bytesWritten = 0
		}
		keyHash, _ := GetHash(rec.key)
		compactInfo.idx.Set(keyHash, indexRec)
	}
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
		reader, err := NewSSTReader(seqNum, compactInfo.partitionId)
		if err != nil {
			panic(err)
		}
		reader.startKey = sstWriter.startKey
		reader.endKey = sstWriter.endKey
		reader.noOfWriteReq = sstWriter.noOfWriteReq
		reader.noOfDelReq = sstWriter.noOfDelReq
		compactInfo.newSSTReaders = append(compactInfo.newSSTReaders, reader)
	}
}

//does followings -
//1. partition index with new compacted ssts index
//2. update active sstReaderMap with newly created ssts
//3. remove compacted sst's from active sstReaderMap
//4. write manifest
func (pInfo *PartitionInfo) updatePartition() { //TODO - test cases
	manifestRecs := make([]ManifestRec, 0, 8)
	pInfo.levelLock.Lock()
	compactInfo := pInfo.activeCompaction
	//update active sstReaderMap with newly created ssts
	for _, newReader := range compactInfo.newSSTReaders {
		mf1 := ManifestRec{
			partitionId: pInfo.partitionId,
			seqNum:      newReader.SeqNm,
			levelNum:    compactInfo.nextLevel,
			fop:         DefaultConstants.fileCreate,
			fileType:    DefaultConstants.sstFileType,
		}
		manifestRecs = append(manifestRecs, mf1)
		pInfo.sstReaderMap[newReader.SeqNm] = newReader
		pInfo.levelsInfo[compactInfo.nextLevel].sstSeqNums[newReader.SeqNm] = struct{}{}
	}
	pInfo.levelLock.Unlock()

	//update active index with new index data (new ssts)
	iterator := compactInfo.idx.NewIterator()
	for iterator.Next() {
		next := iterator.Value()
		tmpKeyHash := next.Key()
		tmpIndexRec := next.Value()

		indexVal := pInfo.index.Get(tmpKeyHash)
		if indexVal == nil {
			pInfo.index.Set(tmpKeyHash, tmpIndexRec)
		} else {
			idxRec := indexVal.Value()
			if idxRec.TS > tmpIndexRec.TS { //if there is a latest write
				continue
			}
			pInfo.index.Set(tmpKeyHash, tmpIndexRec)
		}
	}

	deleteReaders := make([]SSTReader, 0, 8)
	//remove compacted sst's from active sstReaderMap
	pInfo.levelLock.Lock()
	thisLevelInfo := pInfo.levelsInfo[compactInfo.thisLevel]
	nextLevelInfo := pInfo.levelsInfo[compactInfo.nextLevel]
	for _, reader := range compactInfo.botLevelSST {
		activeSSTReader := pInfo.sstReaderMap[reader.SeqNm]
		activeSSTReader.Close()
		delete(thisLevelInfo.sstSeqNums, reader.SeqNm)
		delete(pInfo.sstReaderMap, reader.SeqNm)
		mf1 := ManifestRec{
			partitionId: pInfo.partitionId,
			seqNum:      reader.SeqNm,
			levelNum:    compactInfo.thisLevel,
			fop:         DefaultConstants.fileDelete,
			fileType:    DefaultConstants.sstFileType,
		}
		deleteReaders = append(deleteReaders, reader)
		manifestRecs = append(manifestRecs, mf1)
	}

	for _, reader := range compactInfo.topLevelSST {
		activeSSTReader := pInfo.sstReaderMap[reader.SeqNm]
		activeSSTReader.Close()
		delete(nextLevelInfo.sstSeqNums, reader.SeqNm)
		delete(pInfo.sstReaderMap, reader.SeqNm)
		mf1 := ManifestRec{
			partitionId: pInfo.partitionId,
			seqNum:      reader.SeqNm,
			levelNum:    compactInfo.nextLevel,
			fop:         DefaultConstants.fileDelete,
			fileType:    DefaultConstants.sstFileType,
		}
		deleteReaders = append(deleteReaders, reader)
		manifestRecs = append(manifestRecs, mf1)
	}
	pInfo.levelLock.Unlock()
	writeManifest(manifestRecs) //TODO - check for consistent - can we name sst and temp and post manifest write confirm it to permanent
	for _, reader := range deleteReaders {
		err := deleteSST(reader.partitionId, reader.SeqNm)
		if err != nil {
			panic(err)
		}
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

	pInfo.levelLock.RLock()
	defer pInfo.levelLock.RUnlock()

	thisLevelSSTSeqNums := pInfo.levelsInfo[compactInfo.thisLevel].sstSeqNums
	nextLevelSSTSeqNums := pInfo.levelsInfo[compactInfo.nextLevel].sstSeqNums
	thisLevelSortedSeqNums := pInfo.sortedSeqNums(thisLevelSSTSeqNums) //sorted ssts seq_num of thisLevel

	thisLevelSKey := ""
	thisLevelEKey := ""

	//collect all sst's from this level until total overlapped compact files are less than maxSSTCompact
	//fist file of this level gets added automatically irrespective of number of match with next level
	nextLevelOverlappedSSTMap := make(map[uint32]SSTReader)
	for _, sstSeq := range thisLevelSortedSeqNums {
		sstReader := pInfo.sstReaderMap[sstSeq]
		sKey := string(sstReader.startKey)
		eKey := string(sstReader.endKey)
		overlappedSST := pInfo.overlappedSSTReaders(nextLevelSSTSeqNums, sKey, eKey)
		//fmt.Println("overlappedSST- ", overlappedSST, " for key range- ", sKey, eKey)
		if len(overlappedSST) != 0 {
			if len(compactInfo.botLevelSST) == 0 { //first file must get included irrespective of overlap count
				//fmt.Println(sstReader)
				compactInfo.botLevelSST = append(compactInfo.botLevelSST, sstReader)
				fillOverlappedMap(nextLevelOverlappedSSTMap, overlappedSST)
				thisLevelSKey = sKey
				thisLevelEKey = eKey
			} else {
				if len(overlappedSST) > DefaultConstants.maxSSTCompact {
					continue
				}
				tmp_sKey, tmp_eKey := keyRange(sKey, eKey, thisLevelSKey, thisLevelEKey)
				//fmt.Println("tmp_sKey, tmp_eKey :", tmp_sKey, tmp_eKey)
				overlappedSST := pInfo.overlappedSSTReaders(nextLevelSSTSeqNums, tmp_sKey, tmp_eKey)
				if len(overlappedSST) > 0 && len(overlappedSST) <= DefaultConstants.maxSSTCompact {
					//fmt.Println(sstReader)
					compactInfo.botLevelSST = append(compactInfo.botLevelSST, sstReader)
					fillOverlappedMap(nextLevelOverlappedSSTMap, overlappedSST)
					thisLevelSKey = tmp_sKey
					thisLevelEKey = tmp_eKey
				}
			}
		}
	}

	//if no overlapped found for individual level 0 files, take all bot files and do compact
	if len(compactInfo.botLevelSST) == 0 {
		fmt.Println("No overlapped tables found in level 1, all level 0 files will be compacted")
		for sstSeqNum := range thisLevelSSTSeqNums {
			reader := pInfo.sstReaderMap[sstSeqNum]
			compactInfo.botLevelSST = append(compactInfo.botLevelSST, reader)
			thisLevelSKey, thisLevelEKey = keyRange(string(reader.startKey), string(reader.endKey), thisLevelSKey, thisLevelEKey)
		}
	}

	overlappedSST := pInfo.overlappedSSTReaders(nextLevelSSTSeqNums, thisLevelSKey, thisLevelEKey)
	if len(overlappedSST) > 0 {
		fillOverlappedMap(nextLevelOverlappedSSTMap, overlappedSST)
	}

	for _, sstReader := range nextLevelOverlappedSSTMap {
		compactInfo.topLevelSST = append(compactInfo.topLevelSST, sstReader)
	}
	//New SST file must be _temp..once we complete write for the new SSTs we must rename this file (removing _temp) + load index and write delete meta in manifesto
	return thisLevelSKey, thisLevelEKey
}

func fillOverlappedMap(overlappedMap map[uint32]SSTReader, readers []SSTReader) {
	for _, reader := range readers {
		overlappedMap[reader.SeqNm] = reader
	}
}

func (compactInfo *CompactInfo) fillTopLevels() (string, string) {
	//for bot level > 0, pick ssts if count is more than levelMaxSST
	pInfo := partitionInfoMap[compactInfo.partitionId]
	pInfo.levelLock.RLock()
	defer pInfo.levelLock.RUnlock()

	levelMax := DefaultConstants.levelMaxSST[compactInfo.thisLevel]
	thisLevelSSTSeqNums := pInfo.levelsInfo[compactInfo.thisLevel].sstSeqNums
	nextLevelSSTSeqNums := pInfo.levelsInfo[compactInfo.nextLevel].sstSeqNums
	thisLevelSortedSeqNums := pInfo.sortedSeqNums(thisLevelSSTSeqNums)

	thisLevelSKey := ""
	thisLevelEKey := ""

	var count uint32 = 0
	for _, sstSeq := range thisLevelSortedSeqNums {
		if uint32(len(thisLevelSSTSeqNums))-count < levelMax {
			break
		}
		sstReader := pInfo.sstReaderMap[uint32(sstSeq)]
		compactInfo.botLevelSST = append(compactInfo.botLevelSST, sstReader)
		sKey, eKey := string(sstReader.startKey), string(sstReader.endKey)
		thisLevelSKey, thisLevelEKey = keyRange(sKey, eKey, thisLevelSKey, thisLevelEKey)
		count++
	}
	overlappedSST := pInfo.overlappedSSTReaders(nextLevelSSTSeqNums, thisLevelSKey, thisLevelEKey)
	compactInfo.topLevelSST = overlappedSST
	return thisLevelSKey, thisLevelEKey
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

func (pInfo *PartitionInfo) sortedSeqNums(sstSeqMap map[uint32]struct{}) []uint32 {
	sortedKey := make([]uint32, 0, 8)
	for k := range sstSeqMap {
		sortedKey = append(sortedKey, k)
	}
	//1. sort based on number of delete request in desc order
	//2. if del req are equal sort based on sst seq num in asc order
	sort.Slice(sortedKey, func(i, j int) bool {
		delToWrite_i := computeDeleteToWriteRatio(pInfo.sstReaderMap[sortedKey[i]].noOfDelReq,
			pInfo.sstReaderMap[sortedKey[i]].noOfWriteReq)
		delToWrite_j := computeDeleteToWriteRatio(pInfo.sstReaderMap[sortedKey[j]].noOfDelReq,
			pInfo.sstReaderMap[sortedKey[j]].noOfWriteReq)
		if delToWrite_i > delToWrite_j {
			return true
		}
		return pInfo.sstReaderMap[sortedKey[i]].SeqNm < pInfo.sstReaderMap[sortedKey[j]].SeqNm
	})
	return sortedKey
}

func computeDeleteToWriteRatio(deleteCount uint64, writeCount uint64) int {
	return (int)(deleteCount / (deleteCount + writeCount))
}

func (pInfo *PartitionInfo) overlappedSSTReaders(sstSeqNums map[uint32]struct{}, sKey string, eKey string) []SSTReader {
	overlappedSST := make([]SSTReader, 0, 8)
	for sstSeqNum := range sstSeqNums {
		if overlap(sKey, eKey, string(pInfo.sstReaderMap[sstSeqNum].startKey), string(pInfo.sstReaderMap[sstSeqNum].endKey)) {
			overlappedSST = append(overlappedSST, pInfo.sstReaderMap[sstSeqNum])
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
	pInfo.levelLock.Lock()
	defer pInfo.levelLock.Unlock()

	for _, reader := range compactInfo.botLevelSST {
		delete(pInfo.levelsInfo[compactInfo.thisLevel].sstSeqNums, reader.SeqNm)
		pInfo.levelsInfo[compactInfo.nextLevel].sstSeqNums[reader.SeqNm] = struct{}{}
		mf1 := ManifestRec{
			partitionId: pInfo.partitionId,
			levelNum:    compactInfo.nextLevel,
			seqNum:      reader.SeqNm,
			fop:         DefaultConstants.fileCreate,
			fileType:    DefaultConstants.sstFileType,
		}
		writeManifest([]ManifestRec{mf1})
	}
}

func initCompactInfo(level int, partId int) *CompactInfo {
	return &CompactInfo{
		partitionId:   partId,
		thisLevel:     level,
		nextLevel:     level + 1,
		botLevelSST:   make([]SSTReader, 0, 8),
		topLevelSST:   make([]SSTReader, 0, 8),
		newSSTReaders: make([]SSTReader, 0, 100),
		idx:           index.NewIndex(),
		heap:          make([]*HeapEle, 0, 10000),
	}
}

func (pInfo *PartitionInfo) level0PossibleCompaction() {
	if len(pInfo.levelsInfo[0].sstSeqNums) >= 4 {
		publishCompactTask(&CompactInfo{
			partitionId: pInfo.partitionId,
			thisLevel:   0,
		})
	}
}

//TODO - if a level(l) has delete request and no write req present in level > l then remove while compaction
