package bladedb

import (
	"bladedb/sklist"
	"fmt"
	"github.com/stretchr/testify/require"
	"grpc/utils"
	"io/ioutil"
	"log"
	"math"
	"os"
	"testing"
)

func TestReadRec(t *testing.T) {
	partitionId := 0
	dir, err := ioutil.TempDir("", "sstLoadTest")
	if err != nil {
		log.Fatal(err)
	}
	// update SSTDir to temp directory
	SSTDir = dir
	sReader1, sReader2 := prepareInputSSTs(dir, partitionId)
	//sReader1 writeReq - 0Key_ to 19Key_
	//sReader2 writeDel - 10Key_ to 24Key_
	defer os.Remove(sReader1.file.Name())
	defer os.Remove(sReader2.file.Name())
	defer os.RemoveAll(dir)

	sstRec1, err := sReader1.ReadRec(0)
	require.NotNil(t, sstRec1, "Expected valid record from sst file")

	sstRec2, err := sReader1.ReadRec(math.MaxInt64)
	require.Nil(t, sstRec2, "Expected nil response for offset greater than limit")
}

func TestLoadSSTRec_Mix_Delele_And_Write(t *testing.T) {
	partitionId := 0
	dir, err := ioutil.TempDir("", "sstLoadTest")
	if err != nil {
		log.Fatal(err)
	}
	// update SSTDir to temp directory
	SSTDir = dir
	index := sklist.New()

	sReader1, sReader2 := prepareInputSSTs(dir, partitionId)
	//sReader1 writeReq - 0Key_ to 19Key_
	//sReader2 writeDel - 10Key_ to 24Key_
	defer os.Remove(sReader1.file.Name())
	defer os.Remove(sReader2.file.Name())
	defer os.RemoveAll(dir)

	sReader1.loadSSTRec(index)
	sReader2.loadSSTRec(index)

	expectedIndexKeys := make(map[string]interface{}, 1)
	expectedIndexKeys["0Key_"] = struct{}{}
	expectedIndexKeys["9Key_"] = struct{}{}
	expectedIndexKeys["2Key_"] = struct{}{}
	expectedIndexKeys["3Key_"] = struct{}{}
	expectedIndexKeys["1Key_"] = struct{}{}
	expectedIndexKeys["8Key_"] = struct{}{}
	expectedIndexKeys["6Key_"] = struct{}{}
	expectedIndexKeys["5Key_"] = struct{}{}
	expectedIndexKeys["7Key_"] = struct{}{}
	expectedIndexKeys["4Key_"] = struct{}{}

	require.True(t, index.Length == len(expectedIndexKeys),
		fmt.Sprintf("Expected %d numbers of keys in index", index.Length))

	for key, _ := range expectedIndexKeys {
		hash, _ := utils.GetHash([]byte(key))
		exists := index.Get(hash) != nil
		require.True(t, exists, fmt.Sprintf("Expected key %s (hash: %s) is missing from index", key, hash))
	}
}

func TestLoadSSTRec_Only_Write(t *testing.T) {
	partitionId := 0
	dir, err := ioutil.TempDir("", "sstLoadTest")
	if err != nil {
		log.Fatal(err)
	}
	// update SSTDir to temp directory
	SSTDir = dir
	index := sklist.New()

	sReader1, sReader2 := prepareInputSSTs(dir, partitionId)
	//sReader1 writeReq - 0Key_ to 19Key_
	defer os.Remove(sReader1.file.Name())
	defer os.Remove(sReader2.file.Name())
	defer os.RemoveAll(dir)

	sReader1.loadSSTRec(index)

	require.True(t, index.Length == 20,
		fmt.Sprintf("Expected %d numbers of keys in index", index.Length))
	//random exists check

	hash, _ := GetHash([]byte("0Key_"))
	require.NotNil(t, index.Get(hash), fmt.Sprintf("expected key %s not in index", "0Key_"))

	hash1, _ := GetHash([]byte("8Key_"))
	require.NotNil(t, index.Get(hash1), fmt.Sprintf("expected key %s not in index", "8Key_"))

	hash2, _ := GetHash([]byte("19Key_"))
	require.NotNil(t, index.Get(hash2), fmt.Sprintf("expected key %s not in index", "19Key_"))
}

func TestLoadSSTRec_Only_Delete(t *testing.T) {
	partitionId := 0
	dir, err := ioutil.TempDir("", "sstLoadTest")
	if err != nil {
		log.Fatal(err)
	}
	// update SSTDir to temp directory
	SSTDir = dir
	index := sklist.New()

	sReader1, sReader2 := prepareInputSSTs(dir, partitionId)
	//sReader1 deleteReq - 10Key_ to 24Key_
	defer os.Remove(sReader1.file.Name())
	defer os.Remove(sReader2.file.Name())
	defer os.RemoveAll(dir)

	sReader2.loadSSTRec(index)

	require.True(t, index.Length == 0,
		fmt.Sprintf("Expected %d numbers of keys in index", index.Length))
}
