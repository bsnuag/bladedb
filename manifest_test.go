package bladedb

import (
	"github.com/niubaoshu/gotiny"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"log"
	"os"
	"testing"
)

func TestManifestReplay(t *testing.T) {
	dir, err := ioutil.TempDir("", "manifestTest")
	if err != nil {
		log.Fatal(err)
	}
	file, err := ioutil.TempFile(dir, "MANIFEST.*.txt")
	defer os.Remove(file.Name())
	defer os.RemoveAll(dir)

	if err != nil {
		log.Fatal(err)
	}
	//fmt.Println("Created File: " + file.Name())

	manifestFile = &ManifestFile{
		file:     file,
		manifest: nil,
	}
	manifestRecs := getManifestRecs()

	err = writeManifest(manifestRecs)
	if err != nil {
		panic(err)
	}

	err = replay()
	if err != nil {
		panic(err)
	}
	require.Equal(t, len(manifestRecs), len(manifestFile.manifest.sstManifest[0].manifestRecs)+len(manifestFile.manifest.logManifest[0].manifestRecs),
		"total recs write and total recs read in replay should match")
}

func TestManifestWrite(t *testing.T) {
	dir, err := ioutil.TempDir("", "manifestTest")
	if err != nil {
		log.Fatal(err)
	}
	file, err := ioutil.TempFile(dir, "MANIFEST.*.txt")
	defer os.Remove(file.Name())
	defer os.RemoveAll(dir)

	if err != nil {
		log.Fatal(err)
	}
	//fmt.Println("Created File: " + file.Name())

	manifestFile = &ManifestFile{
		file: file,
	}
	manifestRecs := getManifestRecs()
	writeManifest(manifestRecs)

	file.Seek(0, 0)
	b1 := make([]byte, 50)
	b2 := make([]byte, 50)

	_, err = file.Read(b1)
	if err != nil {
		panic(err)
	}

	_, err = file.Read(b2)
	if err != nil {
		panic(err)
	}

	um1 := ManifestRec{}
	um2 := ManifestRec{}
	gotiny.Unmarshal(b1, &um1)
	gotiny.Unmarshal(b2, &um2)

	//fmt.Println("Unmarshal rec1: ", um1)
	//fmt.Println("Unmarshal rec2: ", um2)

	err = file.Close()
	if err != nil {
		panic(err)
	}
	require.Equal(t, manifestRecs[0], um1, "m1 doesn't match with um1")
	require.Equal(t, manifestRecs[1], um2, "m2 doesn't match with um2")
}

func getManifestRecs() []ManifestRec {
	m1 := ManifestRec{
		partitionId: 0,
		seqNum:      1,
		levelNum:    1,
		fop:         DefaultConstants.fileCreate,
		fileType:    0,
	}

	m2 := ManifestRec{
		partitionId: 0,
		seqNum:      2,
		levelNum:    2,
		fop:         DefaultConstants.fileDelete,
		fileType:    1,
	}

	return []ManifestRec{m1, m2}
}

func TestInitManifest(t *testing.T) {
	ManifestFileName = "data/test_manifest"
	if err := initManifest(); err != nil {
		panic(err)
	}
	defer func() {
		os.Remove(ManifestFileName)
		manifestFile = nil
	}()
	require.Nil(t, manifestFile.manifest, "Expected no manifest built on empty manifest file")
}
