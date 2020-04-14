package bladedb

import "fmt"

func PrintPartitionStats() {
	fmt.Println("------------------------------------X Printing DB Stats X------------------------------------")

	for partId, pInfo := range partitionInfoMap {
		fmt.Println(fmt.Sprintf("\n\n--------------- Stats for PartitionId: %d ---------------", partId))
		fmt.Println(fmt.Sprintf("PartitionId: %d", partId))
		fmt.Println(fmt.Sprintf("No of Keys in Index: %d", pInfo.index.Size()))
		fmt.Println(fmt.Sprintf("No of Keys in active Mem: %d", pInfo.memTable.Size()))
		var inactiveMemSize int64 = 0
		for _, inactiveLogDetail := range pInfo.inactiveLogDetails {
			inactiveMemSize += inactiveLogDetail.MemTable.Size()
		}
		fmt.Println(fmt.Sprintf("No of Keys in inactive Mem: %d", inactiveMemSize))
		for l, lInfo := range pInfo.levelsInfo {
			fmt.Println(fmt.Sprintf("In Level: %d, Number of SSTable: %d", l, len(lInfo.sstSeqNums)))
			fmt.Println(fmt.Sprintf("In Level: %d, SSTable SeqNums: %v", l, lInfo.sstSeqNums))
		}
		var totalWrite, totalDelete uint32 = 0, 0
		for num, reader := range pInfo.sstReaderMap {
			fmt.Println(fmt.Sprintf("SST SeqNum: %d, Total Write: %d, Total Delete: %d", num,
				reader.noOfWriteReq, reader.noOfDelReq))
			totalWrite += reader.noOfWriteReq
			totalDelete += reader.noOfDelReq
		}
		fmt.Println(fmt.Sprintf("PartId: %d, Total Write: %d, Total Delete: %d", partId,
			totalWrite, totalDelete))
		fmt.Println()
		fmt.Println()
	}
}

type PartitionStat struct {
	pId               int
	keys              int64
	inactiveMemTables int
	sstCount          int64
}

func GetPartitionStats() []PartitionStat {
	pStats := make([]PartitionStat, DefaultConstants.noOfPartitions)
	for partitionId := 0; partitionId < DefaultConstants.noOfPartitions; partitionId++ {
		pStat := PartitionStat{
			pId:               partitionId,
			keys:              partitionInfoMap[partitionId].partitionNoOfKeys(),
			inactiveMemTables: len(partitionInfoMap[partitionId].inactiveLogDetails),
			sstCount:          int64(len(partitionInfoMap[partitionId].sstReaderMap)),
		}
		pStats[partitionId] = pStat
	}
	return pStats
}

func (pInfo PartitionInfo) partitionNoOfKeys() int64 {
	noOfKeys := int64(pInfo.index.Size()) //noOfKeys is combination of active keys + keys which are deleted

	pInfo.readLock.Lock()
	noOfKeys += pInfo.memTable.Size()
	for _, inactiveMem := range pInfo.inactiveLogDetails {
		noOfKeys += inactiveMem.MemTable.Size()
	}
	pInfo.readLock.Unlock()

	return noOfKeys
}
