package bladedb

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"log"
	"math"
	"os"
	"testing"
)

func setupSSTTest(partitionId int) (s1 SSTReader, s2 SSTReader, tearTest func()) {
	dir, err := ioutil.TempDir("", "sstLoadTest")
	if err != nil {
		log.Fatal(err)
	}
	db = &bladeDB{}
	db.config = &Config{}
	db.config.DataDir = dir
	sReader1, sReader2 := prepareInputSSTs(partitionId, 0)
	return sReader1, sReader2, func() {
		os.Remove(sReader1.fileName)
		os.Remove(sReader2.fileName)
		os.RemoveAll(db.config.DataDir)
		db = nil
	}
}

func TestSSTReadRec(t *testing.T) {
	partitionId := 0
	sReader1, _, tearTest := setupSSTTest(partitionId)
	defer tearTest()
	n1, _ := sReader1.ReadRec(0)
	require.True(t, n1 > 0, "Expected valid record from sst file")

	n2, _ := sReader1.ReadRec(math.MaxUint32)
	require.True(t, n2 == 0, "Expected nil response for offset greater than limit")
}

func TestLoadSSTRec_Mix_Delele_And_Write(t *testing.T) {
	partitionId := 0
	sReader1, sReader2, tearTest := setupSSTTest(partitionId)
	defer tearTest()
	index := NewIndex()

	sReader1.loadSSTRec(index)
	sReader2.loadSSTRec(index)

	expectedIndexKeys := make(map[string]interface{}, 1)
	for i := 0; i <= 9; i++ {
		expectedIndexKeys[fmt.Sprintf("%dKey_", i)] = struct{}{}
	}
	require.True(t, index.Size() == len(expectedIndexKeys),
		fmt.Sprintf("Expected %d numbers of keys in index", index.Size()))

	for key, _ := range expectedIndexKeys {
		hash := Hash([]byte(key))
		_, ok := index.Get(hash)
		require.True(t, ok, fmt.Sprintf("Expected key %s (hash: %o) is missing from index", key, hash))
	}
}

func TestLoadSSTRec_Only_Write(t *testing.T) {
	partitionId := 0
	sReader1, _, tearTest := setupSSTTest(partitionId)
	defer tearTest()
	index := NewIndex()

	sReader1.loadSSTRec(index)

	require.True(t, index.Size() == 20,
		fmt.Sprintf("Expected %d numbers of keys in index", index.Size()))
	//random exists check

	hash := Hash([]byte("0Key_"))
	_, ok := index.Get(hash)
	require.True(t, ok, fmt.Sprintf("expected key %s not in index", "0Key_"))

	hash1 := Hash([]byte("8Key_"))
	_, ok = index.Get(hash1)
	require.NotNil(t, ok, fmt.Sprintf("expected key %s not in index", "8Key_"))

	hash2 := Hash([]byte("19Key_"))
	_, ok = index.Get(hash2)
	require.NotNil(t, ok, fmt.Sprintf("expected key %s not in index", "19Key_"))
}

func TestLoadSSTRec_Only_Delete(t *testing.T) {
	partitionId := 0
	_, sReader2, tearTest := setupSSTTest(partitionId)
	defer tearTest()
	index := NewIndex()

	sReader2.loadSSTRec(index)

	require.True(t, index.Size() == 0,
		fmt.Sprintf("Expected %d numbers of keys in index", index.Size()))
}
