package bladedb

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"sync"
	"testing"
)

func setupDBTest() (configFile string, tearTest func()) {
	config := Config{LogFileMaxLen: DefaultLogFileMaxLen,
		LogDir:              "dbTestLog/",
		DataDir:             "dbTestSST/",
		WalFlushPeriodInSec: 10,
		CompactWorker:       0,
		NoOfPartitions:      8,
		MemFlushWorker:      8,
	}
	bytes, _ := yaml.Marshal(&config)
	cFile, err := ioutil.TempFile("", "abcdef.yaml")
	if err != nil {
		panic(err)
	}
	if _, err = cFile.Write(bytes); err != nil {
		panic(err)
	}
	if err = cFile.Close(); err != nil {
		panic(err)
	}

	return cFile.Name(), func() {
		os.RemoveAll(config.DataDir)
		os.RemoveAll(config.LogDir)
		os.RemoveAll(cFile.Name())
		db = nil
	}
}

//writes & closes - 1. test db reload from manifest is correct, 2. test if deleted keys re-appears,
func TestDBWrite_With_Reload_MemFlush(t *testing.T) {
	configFilePath, tearTestFun := setupDBTest()
	defer tearTestFun()

	wg := sync.WaitGroup{}
	Open(configFilePath)
	wg.Add(5000)
	for i := 0; i < 5000; i++ {
		go func(j int) { // goroutines are used to test if parallel write to log encoder buffer is working properly
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

	Open(configFilePath) //reopen db
	wg.Add(1000)
	for i := 0; i < 1000; i++ {
		go func(j int) {
			Delete([]byte(fmt.Sprintf("Key:%d", j)))
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

	Open(configFilePath) //reopen db
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
