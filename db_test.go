package bladedb

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"log"
	"os"
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

func TestDBWrite_With_Reload(t *testing.T) {
	defer setupDBTest()()

	Open()
	for i := 0; i < 2000; i++ {
		Put(fmt.Sprintf("Key:%d", i), []byte(fmt.Sprintf("Value:%d", i)))
	}
	Drain()           //similar to close DB
	openErr := Open() //reopen db
	require.Nil(t, openErr)
	for i := 0; i < 2000; i++ {
		bytes, _ := Get(fmt.Sprintf("Key:%d", i))
		require.NotNil(t, bytes)
	}
	Drain()
}

//writes & closes - 1. test db reload from manifest is correct, 2. test if deleted keys re-appears,
func TestDBWrite_With_Reload_MemFlush(t *testing.T) {
	defer setupDBTest()()

	DefaultConstants.noOfPartitions = 1
	DefaultConstants.memFlushWorker = 1 //override

	Open()
	for i := 0; i < 5000; i++ {
		Put(fmt.Sprintf("Key:%d", i), []byte(fmt.Sprintf("Value:%d", i)))
	}
	pStats := GetPartitionStats()
	require.True(t, pStats[0].keys == 5000)
	require.True(t, pStats[0].sstCount == 0)
	require.True(t, pStats[0].inactiveMemTables == 0)
	Drain() //similar to close DB

	Open() //reopen db
	for i := 0; i < 1000; i++ {
		Remove(fmt.Sprintf("Key:%d", i))
	}
	for i := 0; i < 5000; i++ {//remove this loop later
		key:=fmt.Sprintf("Key:%d", i)
		bytes, _ := Get(key)
		if i < 1000 {
			require.Nil(t, bytes)
		} else {
			require.NotNil(t, bytes)
		}
	}

	pStats = GetPartitionStats()
	require.True(t, pStats[0].keys == 5000) //4000 index + 1000 memtable
	require.True(t, pStats[0].sstCount == 1)
	require.True(t, pStats[0].inactiveMemTables == 0)
	Drain() //1000 delete request will go to SST (2nd SST)

	Open() //reopen db

	recs := partitionInfoMap[0].memTable.Recs()
	iterator := recs.NewIterator()

	for iterator.Next() {
		val := iterator.Value()
		if val.Key() == "Key:1010" {
			break
		}
	}
	indexIt := partitionInfoMap[0].index.NewIterator()
	for indexIt.Next() {
		val := indexIt.Value()
		if val.Key() == "Key:1110" {
			break
		}
	}
	for i := 0; i < 5000; i++ {
		key:=fmt.Sprintf("Key:%d", i)
		bytes, _ := Get(key)
		if i < 1000 {
			require.Nil(t, bytes)
		} else {
			require.NotNil(t, bytes)
		}
	}
	pStats = GetPartitionStats()
	require.True(t, pStats[0].keys == 4000)
	require.True(t, pStats[0].sstCount == 2)
	require.True(t, pStats[0].inactiveMemTables == 0)

	Drain()
}
