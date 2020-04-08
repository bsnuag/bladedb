package bladedb

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"
)

func setupTest() (tearTest func()) {
	sstdir, err := ioutil.TempDir("", "partitionRWTestSST")
	if err != nil {
		log.Fatal(err)
	}
	logdir, err := ioutil.TempDir("", "partitionRWTestLog")
	if err != nil {
		log.Fatal(err)
	}
	manifestfile, err := ioutil.TempFile("", "partitionRWTestManifest")
	if err != nil {
		log.Fatal(err)
	}

	// update SSTDir to temp directory
	SSTDir = sstdir
	LogDir = logdir
	ManifestFileName = manifestfile.Name()
	DefaultConstants.compactWorker = 0
	DefaultConstants.memFlushWorker = 0
	DefaultConstants.noOfPartitions = 3

	return func() {
		os.RemoveAll(sstdir)
		os.RemoveAll(logdir)
		os.RemoveAll(manifestfile.Name())
		partitionInfoMap = make(map[int]*PartitionInfo) //clear index map
	}
}

//partition_writer test cases
func TestPartition_Delete_Write_Read_CountVerify(t *testing.T) {
	defer setupTest()()

	if err := Open(); err != nil {
		panic(err)
	}

	for i := 0; i < 1000; i++ {
		value, _ := Remove([]byte(fmt.Sprintf("Key:%d", i)))
		require.Nil(t, value) //no write so result for delete should be nil
	}

	for i := 0; i < 10000; i++ {
		Put([]byte(fmt.Sprintf("Key:%d", i)), []byte(fmt.Sprintf("Key:%d", i)))
	}

	for i := 9000; i < 10000; i++ {
		value, _ := Remove([]byte(fmt.Sprintf("Key:%d", i)))
		require.NotNil(t, value)
	}

	for i := 0; i < 10000; i++ {
		bytes, _ := Get([]byte(fmt.Sprintf("Key:%d", i)))
		if i >= 9000 {
			require.Nil(t, bytes)
		} else {
			require.NotNil(t, bytes)
		}
	}

	partitionStats := GetPartitionStats()
	totalKeys := totalKeys(partitionStats)
	require.True(t, DefaultConstants.memFlushWorker == 0)
	//deleted keys never goes from scope until key reaches top level during compaction
	require.True(t, totalKeys == 10000)

	//since memflush is turned off - sstreader map should 0,
	// index should 0, levelinfo (level 0) len should be zero
	for _, pInfo := range partitionInfoMap {
		require.True(t, pInfo.index.Size() == 0)
		require.True(t, len(pInfo.sstReaderMap) == 0)
		require.True(t, len(pInfo.levelsInfo[0].sstSeqNums) == 0)
	}
}

func TestPartition_Delete_Write_Read_CountVerify_WithFlush(t *testing.T) {
	defer setupTest()()

	// override default constants
	DefaultConstants.noOfPartitions = 1
	DefaultConstants.memFlushWorker = DefaultConstants.noOfPartitions

	if err := Open(); err != nil {
		panic(err)
	}

	for i := 0; i < 1000; i++ {
		value, _ := Remove([]byte(fmt.Sprintf("Key:%d", i)))
		require.Nil(t, value) //no write so result for delete should be nil
	}

	for i := 0; i < 10000; i++ {
		Put([]byte(fmt.Sprintf("Key:%d", i)), []byte(fmt.Sprintf("Key:%d", i)))
	}

	for i := 9000; i < 10000; i++ {
		value, _ := Remove([]byte(fmt.Sprintf("Key:%d", i)))
		require.NotNil(t, value)
	}

	for i := 0; i < 10000; i++ {
		bytes, _ := Get([]byte(fmt.Sprintf("Key:%d", i)))
		if i >= 9000 {
			require.Nil(t, bytes)
		} else {
			require.NotNil(t, bytes)
		}
	}

	Flush()
	//wait for all active memflush to complete (add counter for ongoing flush)
	time.Sleep(2 * time.Second)

	partitionStats := GetPartitionStats()
	totalNoOfKeys := totalKeys(partitionStats)
	require.True(t, DefaultConstants.memFlushWorker == 1)
	require.True(t, totalNoOfKeys == 9000)
	//since memflush is turned on - sstreader map should 1,
	// index should not 0, levelinfo (level 0) len should not be zero, len of active memtable should be 0
	for _, pInfo := range partitionInfoMap {
		fmt.Println(pInfo.index.Size())
		require.True(t, pInfo.index.Size() == 9000) //deleted keys does not go to index
		require.True(t, pInfo.memTable.Size() == 0)
		require.True(t, len(pInfo.inactiveLogDetails) == 0)
		require.True(t, len(pInfo.sstReaderMap) > 0)
		require.True(t, len(pInfo.levelsInfo[0].sstSeqNums) > 0)
	}

	//at this point all keys are in index and no data in memtable (active and inactive-list)
	//remove few keys now
	for i := 0; i < 1000; i++ {
		value, _ := Remove([]byte(fmt.Sprintf("Key:%d", i)))
		require.NotNil(t, value)
	}

	partitionStats = GetPartitionStats()
	totalNoOfKeys = totalKeys(partitionStats)
	require.True(t, totalNoOfKeys == 9000) //8000 in index + 1000 in active memtable

	for _, pInfo := range partitionInfoMap {
		require.True(t, pInfo.index.Size() == 8000)    //1000 removed
		require.True(t, pInfo.memTable.Size() == 1000) //1000 written to memflush
	}
	//read all keys, all deleted keys must not appear
	for i := 0; i < 10000; i++ {
		bytes, _ := Get([]byte(fmt.Sprintf("Key:%d", i)))
		if i < 1000 || i >= 9000 {
			require.Nil(t, bytes)
		} else {
			require.NotNil(t, bytes)
		}
	}
}

//memflush worker will be inactive but will trigger Flush() method
func TestPartition_Delete_Write_Read_CountVerify_Flush_With0FlushWorker(t *testing.T) {
	defer setupTest()()

	// override default constants
	DefaultConstants.noOfPartitions = 1
	DefaultConstants.memFlushWorker = 0 //no flush worker
	memFlushActive = 0

	if err := Open(); err != nil {
		panic(err)
	}

	for i := 0; i < 1000; i++ {
		value, _ := Remove([]byte(fmt.Sprintf("Key:%d", i)))
		require.Nil(t, value) //no write so result for delete should be nil
	}

	for i := 0; i < 10000; i++ {
		Put([]byte(fmt.Sprintf("Key:%d", i)), []byte(fmt.Sprintf("Key:%d", i)))
	}

	for i := 9000; i < 10000; i++ {
		value, _ := Remove([]byte(fmt.Sprintf("Key:%d", i)))
		require.NotNil(t, value)
	}

	for i := 0; i < 10000; i++ {
		bytes, _ := Get([]byte(fmt.Sprintf("Key:%d", i)))
		if i >= 9000 {
			require.Nil(t, bytes)
		} else {
			require.NotNil(t, bytes)
		}
	}

	Flush() //this will put active memtable to inactive list but not to sst (disk)
	//wait for all active memflush to complete (add counter for ongoing flush)
	time.Sleep(2 * time.Second)

	partitionStats := GetPartitionStats()
	totalNoOfKeys := totalKeys(partitionStats)
	require.True(t, DefaultConstants.memFlushWorker == 0)
	require.True(t, totalNoOfKeys == 10000) //removed opr was writen into mem (before Flush() call), not removed since they are not in index
	for _, pInfo := range partitionInfoMap {
		fmt.Println(pInfo.index.Size())
		require.True(t, pInfo.index.Size() == 0) //data is not flushed yet, index size 0
		require.True(t, pInfo.memTable.Size() == 0)
		require.True(t, pInfo.inactiveLogDetails[0].MemTable.Size() == 10000)
		require.True(t, len(pInfo.inactiveLogDetails) == 1)
		require.True(t, len(pInfo.sstReaderMap) == 0)
		require.True(t, len(pInfo.levelsInfo[0].sstSeqNums) == 0)
	}

	//at this point all keys are in inactive memtable
	//this write will go to active memtable
	for i := 0; i < 1000; i++ {
		value, _ := Remove([]byte(fmt.Sprintf("Key:%d", i)))
		require.NotNil(t, value)
	}

	for _, pInfo := range partitionInfoMap {
		require.True(t, pInfo.index.Size() == 0)
		require.True(t, pInfo.memTable.Size() == 1000) //1000 delete req written to memflush
		require.True(t, pInfo.inactiveLogDetails[0].MemTable.Size() == 10000)
	}
	//read all keys, all deleted keys must not appear
	for i := 0; i < 10000; i++ {
		bytes, _ := Get([]byte(fmt.Sprintf("Key:%d", i)))
		if i < 1000 || i >= 9000 {
			require.Nil(t, bytes)
		} else {
			require.NotNil(t, bytes)
		}
	}
}

func totalKeys(partitionStats []PartitionStat) int64 {
	var len int64 = 0
	for _, pStat := range partitionStats {
		len += pStat.keys
	}
	return len
}
