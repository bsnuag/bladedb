package bladedb

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
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
	fmt.Println(manifestfile.Name())
	if err != nil {
		log.Fatal(err)
	}

	// update SSTDir to temp directory
	SSTDir = sstdir
	LogDir = logdir
	ManifestFileName = manifestfile.Name()
	DefaultConstants.compactActive = 0
	DefaultConstants.memFlushWorker = 0
	DefaultConstants.noOfPartitions = 1

	return func() {
		os.RemoveAll(sstdir)
		os.RemoveAll(logdir)
		os.RemoveAll(manifestfile.Name())
		partitionInfoMap = make(map[int]*PartitionInfo) //clear index map
	}
}

//func TestDBWrite_Reload_From_Manifest(t *testing.T) {
//	setupDBTest()
//	//defer cleanResourceFun()
//
//	Open()
//	for i := 0; i < 200; i++ {
//		Put(fmt.Sprintf("Key:%d", i), []byte(fmt.Sprintf("Value:%d", i)))
//	}
//	Drain() //similar to close DB
//	//reopen db
//	openErr := Open()
//	require.Nil(t, openErr)
//
//	//all data should be there
//	for i := 0; i < 200; i++ {
//		time.Sleep(1*time.Second)
//		bytes, _ := Get(fmt.Sprintf("Key:%d", i))
//		require.NotNil(t, bytes)
//	}
//
//	Drain()
//}

//TODO - complete this
