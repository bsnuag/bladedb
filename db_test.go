package bladedb

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"testing"
)

func setupDBTest() (tearTest func()) {
	sstdir, err := ioutil.TempDir("", "dbTestSST")
	if err != nil {
		log.Fatal(err)
	}
	logdir, err := ioutil.TempDir("", "dbTestLog")
	if err != nil {
		log.Fatal(err)
	}
	manifestfile, err := ioutil.TempFile("", "dbTestManifest")
	if err != nil {
		log.Fatal(err)
	}

	// update SSTDir to temp directory
	SSTDir = sstdir
	LogDir = logdir
	ManifestFileName = manifestfile.Name()
	DefaultConstants.compactWorker = 0
	DefaultConstants.memFlushWorker = 0
	DefaultConstants.noOfPartitions = 1

	return func() {
		closeManifest()
		os.RemoveAll(sstdir)
		os.RemoveAll(logdir)
		os.RemoveAll(manifestfile.Name())
		partitionInfoMap = make(map[int]*PartitionInfo) //clear index map
	}
}

//writes & closes - 1. test db reload from manifest is correct, 2. test if deleted keys re-appears,
func TestDBWrite_With_Reload_MemFlush(t *testing.T) {
	defer setupDBTest()()

	DefaultConstants.noOfPartitions = 8
	DefaultConstants.memFlushWorker = 8 //override

	wg:=sync.WaitGroup{}
	Open()
	wg.Add(5000)
	for i := 0; i < 5000; i++ {
		go func(j int) {// goroutines are used to test if parallel write to log encoder buffer is working properly
			Put([]byte(fmt.Sprintf("Key:%d", j)), []byte(fmt.Sprintf("Value:%d", j)))
			wg.Done()
		}(i)
	}
	wg.Wait()
	pStats := GetPartitionStats()
	require.True(t, totalKeys(pStats) == 5000)
	for _, pStat := range pStats {
		require.True(t, pStat.sstCount == 0)
		require.True(t, pStat.inactiveMemTables == 0)
	}
	Drain() //similar to close DB

	Open() //reopen db
	wg.Add(1000)
	for i := 0; i < 1000; i++ {
		go func(j int) {
			Remove([]byte(fmt.Sprintf("Key:%d", j)))
			wg.Done()
		}(i)
	}
	wg.Wait()
	pStats = GetPartitionStats()
	require.True(t, totalKeys(pStats) == 5000) //4000 index + 1000 memtable
	for _, pStat := range pStats {
		require.True(t, pStat.sstCount == 1)
		require.True(t, pStat.inactiveMemTables == 0)
	}
	Drain() //1000 delete request will go to SST (2nd SST)

	Open() //reopen db
	wg.Add(5000)
	for i := 0; i < 5000; i++ {
		go func(j int) {
			key := []byte(fmt.Sprintf("Key:%d", j))
			bytes, _ := Get(key)
			if j < 1000 {
				require.Nil(t, bytes)
			} else {
				require.NotNil(t, bytes)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	pStats = GetPartitionStats()
	require.True(t, totalKeys(pStats) == 4000)
	for _, pStat := range pStats {
		require.True(t, pStat.sstCount == 2)
		require.True(t, pStat.inactiveMemTables == 0)
	}
	Drain()
}
