package bladedb

import (
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"testing"
)

func setupManifestTest() (configFile string, tearTest func()) {
	config := Config{LogFileMaxLen: DefaultLogFileMaxLen,
		LogDir:              "manifestTestLog/",
		DataDir:             "manifestTestSST/",
		WalFlushPeriodInSec: 10,
		CompactWorker:       0,
		NoOfPartitions:      1,
		MemFlushWorker:      0,
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

func TestManifestReplay(t *testing.T) {
	configFile, tearTest := setupManifestTest()
	defer tearTest()
	defer Drain()
	Open(configFile)
	manifestRecs := getManifestRecs()

	err := writeManifest(manifestRecs)
	if err != nil {
		panic(err)
	}
	err, manifest := replayManifest()
	if err != nil {
		panic(err)
	}
	defManifestRec := ManifestRec{partitionId: 0, seqNum: 1, levelNum: 0, fop: 0, fileType: 0}//written when DB is open()
	require.Equal(t, len(manifestRecs)+1, len(manifest.sstManifest[0].manifestRecs)+len(manifest.logManifest[0].manifestRecs),
		"total recs write and total recs read in replayManifest should match")
	require.Equal(t, manifest.logManifest[0].manifestRecs[1], defManifestRec, "default log created while opening DB didn't match")
	require.Equal(t, manifest.logManifest[0].manifestRecs[2], manifestRecs[0], "replayed manifest didn't match with written manifest")
	require.Equal(t, manifest.sstManifest[0].manifestRecs[2], manifestRecs[1], "replayed manifest didn't match with written manifest")
}

func getManifestRecs() []ManifestRec {
	m1 := ManifestRec{
		partitionId: 0,
		seqNum:      2,
		levelNum:    1,
		fop:         fCreate,
		fileType:    LogFileType,
	}

	m2 := ManifestRec{
		partitionId: 0,
		seqNum:      2,
		levelNum:    2,
		fop:         fDelete,
		fileType:    DataFileType,
	}

	return []ManifestRec{m1, m2}
}
