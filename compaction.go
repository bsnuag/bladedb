package bladedb

import (
	"container/heap"
	"github.com/pkg/errors"
	"sort"
	"time"
)

type HeapEle struct {
	iterator *IterableSST
	rec      SSTRec
}

type heapArr []*HeapEle

func (h heapArr) Len() int { return len(h) }
func (h heapArr) Less(i, j int) bool {
	if string(h[i].rec.key) == string(h[j].rec.key) { //rec with same key having max ts will be at top of heap
		return h[i].rec.ts > h[j].rec.ts
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
	//initialise heap with first rec of all selected ssts
	for _, sstReader := range compactInfo.botLevelSST {
		iterator := sstReader.NewIterator()
		sstRec, _ := iterator.Next()
		ele := &HeapEle{
			iterator: iterator,
			rec:      sstRec,
		}
		compactInfo.heap.Push(ele)
	}

	for _, sstReader := range compactInfo.topLevelSST {
		iterator := sstReader.NewIterator()
		sstRec, _ := iterator.Next()
		ele := &HeapEle{
			iterator: iterator,
			rec:      sstRec,
		}
		compactInfo.heap.Push(ele)
	}

	heap.Init(&compactInfo.heap)
}

type CompactInfo struct {
	partitionId   int
	thisLevel     int
	nextLevel     int //level to which sst will be compacted
	botLevelSST   []*SSTReader
	topLevelSST   []*SSTReader
	newSSTReaders []*SSTReader
	idx           *Index
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
			defaultLogger.Info().Msgf("Received signal to stop compact worker, exiting: %s", workerName)
			break
		}
		defaultLogger.Info().
			Int("partition ", compactTask.partitionId).Int("base level ", compactTask.thisLevel).
			Msgf("compaction request received")

		partitionInfo := partitionInfoMap[compactTask.partitionId]

		partitionInfo.compactLock.Lock() //TODO - check if using mutex inside channel a good practice
		var compactInfo *CompactInfo = nil
		sKey, eKey := "", ""

		if partitionInfo.activeCompaction != nil {
			defaultLogger.Info().
				Int("partition ", compactTask.partitionId).
				Msgf("compaction within partition is sequential, cannot trigger another one")
		} else {
			compactInfo = initCompactInfo(compactTask.thisLevel, compactTask.partitionId)
			partitionInfo.activeCompaction = compactInfo
			if compactInfo.thisLevel == 0 {
				sKey, eKey = compactInfo.fillLevels() //sKey, eKey  - range of query for which we are running compaction
			} else if compactInfo.thisLevel > 0 {
				sKey, eKey = compactInfo.fillTopLevels()
			}
			if sKey == "" && eKey == "" { //this condition will never arise - check it
				defaultLogger.Info().Int("level", compactTask.thisLevel).Int("partition", partitionInfo.partitionId).Msg("no overlapped ssts found")
				partitionInfo.activeCompaction = nil
				compactInfo = nil
			}
		}
		partitionInfo.compactLock.Unlock()

		if compactInfo != nil {
			botLevelSSTNames, topLevelSSTNames, newSSTNames := compactInfo.compactFileNames()
			compactStartTime := time.Now()
			if compactErr := compactInfo.compact(); compactErr != nil { //abort compaction and log error message
				partitionInfo.completeCompaction()
				defaultLogger.Error().
					Err(compactErr).Int("partition", compactInfo.partitionId).
					Msgf("Error in compaction, bottom level(%d) ssts: %v, bottom level(%d) ssts : %v",
						compactInfo.thisLevel, botLevelSSTNames, compactInfo.nextLevel, topLevelSSTNames)
			} else {
				if len(compactInfo.newSSTReaders) > 0 {
					partitionInfo.updatePartition()
				}
				partitionInfo.completeCompaction()
				partitionInfo.checkPossibleCompaction(compactInfo.nextLevel)

				defaultLogger.Info().
					Int("Partition", compactInfo.partitionId).
					Float64("duration (Seconds)", time.Since(compactStartTime).Seconds()).
					Int("Bottom level", compactInfo.thisLevel).
					Strs("Bottom level files ", botLevelSSTNames).
					Int("Top level", compactInfo.nextLevel).
					Strs("Top level files ", topLevelSSTNames).
					Int("Compacted Index Size ", compactInfo.idx.Size()).
					Strs("Resultant SST files ", newSSTNames).
					Msg("Compaction Completed")
			}
		}
	}
}

//check if nextLevel is eligible for compaction
func (pInfo *PartitionInfo) checkPossibleCompaction(level int) {
	if level != DefaultConstants.maxLevel && len(pInfo.levelsInfo[level].sstSeqNums) > int(DefaultConstants.levelMaxSST[level]) {
		publishCompactTask(&CompactInfo{
			partitionId: pInfo.partitionId,
			thisLevel:   level,
		})
	}
}

func (pInfo PartitionInfo) completeCompaction() {
	pInfo.compactLock.Lock()
	pInfo.activeCompaction = nil //complete compaction
	pInfo.compactLock.Unlock()
}

func (compactInfo *CompactInfo) compact() error {
	//if base level is > 0 and base level doesn't have overlapped ssts from top level then just update the level in manifest
	if compactInfo.thisLevel > 0 && len(compactInfo.topLevelSST) == 0 {
		compactInfo.updateLevel()
		return nil
	}

	var indexOffset uint32 = 0
	compactInfo.buildHeap()

	pInfo := partitionInfoMap[compactInfo.partitionId]
	sstWriter, err := pInfo.NewSSTWriter()
	if err != nil {
		return err
	}
	var sstEncoderBuf = make([]byte, uint32(DefaultConstants.noOfPartitions)*logEncoderPartLen)
	totalCompactIndexWrite := 0
	for compactInfo.heap.Len() > 0 {
		_, rec := compactInfo.nextRec()
		if compactInfo.nextLevel == DefaultConstants.maxLevel && rec.recType == DefaultConstants.deleteReq {
			defaultLogger.Info().Msgf("Tombstone rec: %v, with target level=%d, "+
				"avoiding it from adding it to compacted sst", DefaultConstants.maxLevel, rec)
			continue
		}
		indexRec := IndexRec{
			SSTRecOffset:  indexOffset,
			SSTFileSeqNum: sstWriter.SeqNum,
			TS:            rec.ts,
		}
		totalCompactIndexWrite++
		n := rec.SSTEncoder(sstEncoderBuf)
		nn, err := sstWriter.Write(sstEncoderBuf[:n])
		if err != nil {
			return errors.Wrapf(err, "Error while writing to SST during compaction, sstFile: %s", sstWriter.file.Name())
		}
		indexOffset += nn

		if rec.recType == DefaultConstants.deleteReq {
			sstWriter.noOfDelReq++
		} else if rec.recType == DefaultConstants.writeReq {
			sstWriter.noOfWriteReq++
			keyHash := Hash(rec.key)
			compactInfo.idx.Set(keyHash, indexRec) //index update only for write request
		}
		sstWriter.updateKeys(rec.key)

		if indexOffset >= DefaultConstants.maxSSTSize {
			if err := sstWriter.FlushAndClose(); err != nil {
				return err
			}
			reader, err := sstWriter.NewSSTReader()
			if err != nil {
				return err
			}
			compactInfo.newSSTReaders = append(compactInfo.newSSTReaders, &reader)
			sstWriter, _ = pInfo.NewSSTWriter() //all new sst will be in next level
			indexOffset = 0
		}
	}
	if err = sstWriter.FlushAndClose(); err != nil {
		return err
	}

	//if nothing written, delete the file
	if indexOffset == 0 {
		err := deleteSST(pInfo.partitionId, sstWriter.SeqNum)
		if err != nil {
			return err
		}
	} else {
		reader, err := sstWriter.NewSSTReader()
		if err != nil {
			return err
		}
		compactInfo.newSSTReaders = append(compactInfo.newSSTReaders, &reader)
	}
	return nil
}

//does followings -
//1. partition index with new compacted ssts index
//2. update active sstReaderMap with newly created ssts
//3. remove compacted sst's from active sstReaderMap
//4. write manifest
func (pInfo *PartitionInfo) updatePartition() {
	//pInfo.levelLock.Lock()
	compactInfo := pInfo.activeCompaction
	manifestRecs := make([]ManifestRec, 0, len(compactInfo.newSSTReaders)+len(compactInfo.botLevelSST)+len(compactInfo.topLevelSST))
	//update active sstReaderMap with newly created ssts
	newSSTManifests := pInfo.activateNewSSTs()
	manifestRecs = append(manifestRecs, newSSTManifests...)
	//pInfo.levelLock.Unlock()

	//update active index with new index data (new ssts)
	pInfo.updateActiveIndex()

	deleteReadersFun := func(delReader *SSTReader) {
		err := deleteSST(delReader.partitionId, delReader.SeqNm)
		if err != nil {
			panic(err)
		}
	}
	//deleteReaders := make([]SSTReader, 0, len(compactInfo.botLevelSST)+len(compactInfo.topLevelSST))
	//remove compacted sst's from active sstReaderMap
	//pInfo.levelLock.Lock()
	pInfo.readLock.Lock()
	thisLevelInfo := pInfo.levelsInfo[compactInfo.thisLevel]
	nextLevelInfo := pInfo.levelsInfo[compactInfo.nextLevel]
	for _, reader := range compactInfo.botLevelSST {
		reader.Close()
		delete(thisLevelInfo.sstSeqNums, reader.SeqNm)
		delete(pInfo.sstReaderMap, reader.SeqNm)
		mf1 := ManifestRec{
			partitionId: pInfo.partitionId,
			seqNum:      reader.SeqNm,
			levelNum:    compactInfo.thisLevel,
			fop:         DefaultConstants.fileDelete,
			fileType:    DefaultConstants.sstFileType,
		}
		//deleteReaders = append(deleteReaders, reader)
		defer deleteReadersFun(reader)
		manifestRecs = append(manifestRecs, mf1)
	}

	for _, reader := range compactInfo.topLevelSST {
		reader.Close()
		delete(nextLevelInfo.sstSeqNums, reader.SeqNm)
		delete(pInfo.sstReaderMap, reader.SeqNm)
		mf1 := ManifestRec{
			partitionId: pInfo.partitionId,
			seqNum:      reader.SeqNm,
			levelNum:    compactInfo.nextLevel,
			fop:         DefaultConstants.fileDelete,
			fileType:    DefaultConstants.sstFileType,
		}
		//deleteReaders = append(deleteReaders, reader)
		defer deleteReadersFun(reader)
		manifestRecs = append(manifestRecs, mf1)
	}
	//pInfo.levelLock.Unlock()
	pInfo.readLock.Unlock()
	writeManifest(manifestRecs) //TODO - check for consistent - can we name sst and temp and post manifest write confirm it to permanent
	/*for _, reader := range deleteReaders {
		err := deleteSST(reader.partitionId, reader.SeqNm)
		if err != nil {
			panic(err)
		}
	}*/
}

func (compactInfo *CompactInfo) nextRec() (bool, SSTRec) {
	if compactInfo.heap.Len() <= 0 {
		return false, SSTRec{}
	}

	minPop := heap.Pop(&compactInfo.heap).(*HeapEle)
	compactInfo.pushNext(minPop)

	for compactInfo.heap.Len() > 0 {
		//if top is a diff element - break the loop
		if string(compactInfo.heap.Top().(*HeapEle).rec.key) != string(minPop.rec.key) {
			break
		}
		pop := heap.Pop(&compactInfo.heap).(*HeapEle)

		if string(minPop.rec.key) == string(pop.rec.key) {
			if pop.rec.ts > minPop.rec.ts { //TODO - this condition will never be true - will be later after more testing
				minPop = pop
			}
			compactInfo.pushNext(pop)
		}
	}

	return true, minPop.rec
}

func (compactInfo *CompactInfo) pushNext(popEle *HeapEle) bool {
	nextRec, ok := popEle.iterator.Next()

	if !ok {
		defaultLogger.Info().Msgf("reached end of compacting sst file: %s, ", popEle.iterator.mappedFileName)
		return false
	}

	ele := &HeapEle{
		iterator: popEle.iterator,
		rec:      nextRec,
	}
	heap.Push(&compactInfo.heap, ele)
	return true
}

//when current level is 0 and next level is 1
func (compactInfo *CompactInfo) fillLevels() (string, string) {
	pInfo := partitionInfoMap[compactInfo.partitionId]

	//pInfo.levelLock.RLock()
	//defer pInfo.levelLock.RUnlock()

	pInfo.readLock.RLock()
	defer pInfo.readLock.RUnlock()

	thisLevelSSTSeqNums := pInfo.levelsInfo[compactInfo.thisLevel].sstSeqNums
	nextLevelSSTSeqNums := pInfo.levelsInfo[compactInfo.nextLevel].sstSeqNums
	thisLevelSortedSeqNums := pInfo.sortedSeqNums(thisLevelSSTSeqNums) //sorted ssts seq_num of thisLevel

	thisLevelSKey := ""
	thisLevelEKey := ""

	//collect all sst's from this level until total overlapped compact files are less than maxSSTCompact
	//fist file of this level gets added automatically irrespective of number of match with next level
	nextLevelOverlappedSSTMap := make(map[uint32]*SSTReader)
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
		defaultLogger.Info().Msg("No overlapped tables found in level 1, all level 0 files will be compacted")
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

func fillOverlappedMap(overlappedMap map[uint32]*SSTReader, readers []*SSTReader) {
	for _, reader := range readers {
		overlappedMap[reader.SeqNm] = reader
	}
}

func (compactInfo *CompactInfo) fillTopLevels() (string, string) {
	//for bot level > 0, pick ssts if count is more than levelMaxSST
	pInfo := partitionInfoMap[compactInfo.partitionId]
	//pInfo.levelLock.RLock()
	//defer pInfo.levelLock.RUnlock()

	pInfo.readLock.RLock()
	defer pInfo.readLock.RUnlock()

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
	sortedKey := make([]uint32, 0, len(sstSeqMap))
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

func computeDeleteToWriteRatio(deleteCount uint32, writeCount uint32) int {
	return (int)(deleteCount / (deleteCount + writeCount))
}

func (pInfo *PartitionInfo) overlappedSSTReaders(sstSeqNums map[uint32]struct{}, sKey string, eKey string) []*SSTReader {
	overlappedSST := make([]*SSTReader, 0, len(sstSeqNums))
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

func (compactInfo *CompactInfo) updateLevel() {
	pInfo := partitionInfoMap[compactInfo.partitionId]
	//pInfo.levelLock.Lock()
	//defer pInfo.levelLock.Unlock()

	pInfo.readLock.Lock()
	defer pInfo.readLock.Unlock()

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
		botLevelSST:   make([]*SSTReader, 0, 8),
		topLevelSST:   make([]*SSTReader, 0, 8),
		newSSTReaders: make([]*SSTReader, 0, 100),
		idx:           NewIndex(),
		heap:          make([]*HeapEle, 0, 10000),
	}
}

func (pInfo *PartitionInfo) level0PossibleCompaction() { //merge with
	if len(pInfo.levelsInfo[0].sstSeqNums) >= 4 {
		publishCompactTask(&CompactInfo{
			partitionId: pInfo.partitionId,
			thisLevel:   0,
		})
	}
}

func (writer *SSTWriter) updateKeys(key []byte) {
	if writer.startKey == nil {
		writer.startKey = key
	}
	writer.endKey = key
}

//TODO - if a level(l) has delete request and no write req present in level > l then remove while compaction
