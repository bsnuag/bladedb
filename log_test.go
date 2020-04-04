package bladedb

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"testing"
)

func BenchmarkEncoderDecoder(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var off uint32 = 0
		ts := NanoTime()
		logRec1 := LogRecord{1,
			[]byte(fmt.Sprintf("Key%d", i)),
			[]byte(fmt.Sprintf("Key%d", i)),
			ts}
		encoded := logRec1.Encode()
		Decode(&off, encoded)
	}
}

func TestEncoderDecoder_ChainRecord(t *testing.T) {
	key := "abcdef%d"
	value := "abcdef%d"
	ts := NanoTime()

	logRec1 := LogRecord{1, []byte(fmt.Sprintf(key, 1)),
		[]byte(fmt.Sprintf(value, 1)), ts}

	logRec2 := LogRecord{1, []byte(fmt.Sprintf(key, 2)),
		[]byte(fmt.Sprintf(value, 2)), ts}
	encoded1 := logRec1.Encode();
	encoded2 := logRec2.Encode()
	encode := make([]byte, len(encoded1)+len(encoded2))
	copy(encode[0:len(encoded1)], encoded1);
	copy(encode[len(encoded2):], encoded2)
	var off uint32 = 0
	logRec3 := Decode(&off, encode)
	logRec4 := Decode(&off, encode)

	require.True(t, logRec1.recType == logRec3.recType)
	require.True(t, bytes.Equal(logRec1.key, logRec3.key))
	require.True(t, bytes.Equal(logRec1.value, logRec3.value))
	require.True(t, logRec1.ts == logRec3.ts)

	require.True(t, logRec2.recType == logRec4.recType)
	require.True(t, bytes.Equal(logRec2.key, logRec4.key))
	require.True(t, bytes.Equal(logRec2.value, logRec4.value))
	require.True(t, logRec2.ts == logRec4.ts)
}

func TestEncoderDecoder_8ByteData(t *testing.T) {
	key := "abcdef%d"
	value := "abcdef%d"
	testEncoderDecoder(t, key, value, 1000000)
}

func TestEncoderDecoder_128ByteData(t *testing.T) { //close to 128 bytes
	key := strings.Repeat("abc123", 24) + ":%d"
	value := strings.Repeat("xyz987", 24) + ":%d"
	testEncoderDecoder(t, key, value, 1000000)
}

func TestEncoderDecoder_1KByteData(t *testing.T) { //close to 128 bytes
	key := strings.Repeat("sa_123", 170) + ":%d"
	value := strings.Repeat("x_-987", 170) + ":%d"
	testEncoderDecoder(t, key, value, 1000000)
}

func testEncoderDecoder(t *testing.T, key string, value string, N int) {
	for i := 0; i < N; i++ {
		var off uint32 = 0
		ts := NanoTime()
		logRec1 := LogRecord{1, []byte(fmt.Sprintf(key, i)), []byte(fmt.Sprintf(value, i)), ts}
		encoded := logRec1.Encode()
		logRec2 := Decode(&off, encoded)
		require.True(t, logRec1.recType == logRec2.recType)
		require.True(t, bytes.Equal(logRec1.key, logRec2.key))
		require.True(t, bytes.Equal(logRec1.value, logRec2.value))
		require.True(t, logRec1.ts == logRec2.ts)
	}
}

func prepareLogTest() (tearTest func()) {
	sstdir, err := ioutil.TempDir("", "logTestSST")
	if err != nil {
		log.Fatal(err)
	}
	logdir, err := ioutil.TempDir("", "logTestLog")
	if err != nil {
		log.Fatal(err)
	}
	manifestfile, err := ioutil.TempFile("", "logTestManifest")
	if err != nil {
		log.Fatal(err)
	}

	// update SSTDir to temp directory
	SSTDir = sstdir
	LogDir = logdir
	ManifestFileName = manifestfile.Name()
	DefaultConstants.compactWorker = 0
	DefaultConstants.memFlushWorker = 0 //all inactive memtable will in memory+wal, wont be flushed to SST
	DefaultConstants.noOfPartitions = 1
	initManifest()

	return func() {
		closeManifest()
		os.RemoveAll(sstdir)
		os.RemoveAll(logdir)
		os.RemoveAll(manifestfile.Name())
		partitionInfoMap = make(map[int]*PartitionInfo) //clear index map
	}
}

func TestLogWrite_RecoverRead(t *testing.T) {
	defer prepareLogTest()()

	partitionId := 0
	pInfo, _ := NewPartition(partitionId)
	logWriter, _ := newLogWriter(partitionId, pInfo.getNextLogSeq())
	pInfo.logWriter = logWriter
	partitionInfoMap[partitionId] = pInfo

	key := "abcdef%d"
	value := "zbtrql%d"
	writesN := 1000
	for i := 1; i <= writesN; i++ {
		ts := NanoTime()
		pInfo.logWriter.Write([]byte(fmt.Sprintf(key, i)), []byte(fmt.Sprintf(value, i)), ts, 1)
		if i == writesN/2 {
			details, _ := pInfo.logWriter.rollover()
			pInfo.inactiveLogDetails = append(pInfo.inactiveLogDetails, details)
		}
	}

	details, _ := pInfo.logWriter.FlushAndClose()
	pInfo.inactiveLogDetails = append(pInfo.inactiveLogDetails, details)
	for _, logDetails := range pInfo.inactiveLogDetails {
		details, _ := pInfo.loadLogFile(logDetails.FileSeqNum)
		require.NotNil(t, details.WriteOffset)
		require.True(t, details.WriteOffset == fileSize(logDetails.FileName))
		require.True(t, (pInfo.memTable.Size() == int64(writesN/2)) || pInfo.memTable.Size() == int64(writesN))
	}
	require.True(t, len(pInfo.inactiveLogDetails) == 2)
}

func TestLogWrite_RecoverRead_WithOneEmptyFile(t *testing.T) {
	defer prepareLogTest()()

	partitionId := 0
	pInfo, _ := NewPartition(partitionId)
	logWriter, _ := newLogWriter(partitionId, pInfo.getNextLogSeq())
	pInfo.logWriter = logWriter
	partitionInfoMap[partitionId] = pInfo

	key := "abcdef%d"
	value := "zbtrql%d"
	writesN := 100
	for i := 1; i <= writesN; i++ {
		ts := NanoTime()
		pInfo.logWriter.Write([]byte(fmt.Sprintf(key, i)),
			[]byte(fmt.Sprintf(value, i)), ts, 1)
	}
	details, _ := pInfo.logWriter.rollover()
	pInfo.inactiveLogDetails = append(pInfo.inactiveLogDetails, details)

	details, _ = pInfo.logWriter.FlushAndClose()
	pInfo.inactiveLogDetails = append(pInfo.inactiveLogDetails, details)

	require.True(t, len(pInfo.inactiveLogDetails) == 2)

	logDetails_1 := pInfo.inactiveLogDetails[0]
	details_1, _ := pInfo.loadLogFile(logDetails_1.FileSeqNum)
	require.NotNil(t, details_1.WriteOffset)
	require.True(t, details_1.WriteOffset == fileSize(logDetails_1.FileName))
	require.True(t, pInfo.memTable.Size() == int64(writesN))

	logDetails_2 := pInfo.inactiveLogDetails[1]
	details_2, _ := pInfo.loadLogFile(logDetails_2.FileSeqNum)
	require.NotNil(t, details_2.WriteOffset)
	require.True(t, details_2.WriteOffset == fileSize(logDetails_2.FileName))
}

func TestLogRollover(t *testing.T) {
	defer prepareLogTest()()

	partitionId := 0
	pInfo, _ := NewPartition(partitionId)
	logWriter, _ := newLogWriter(partitionId, pInfo.getNextLogSeq())
	pInfo.logWriter = logWriter
	partitionInfoMap[partitionId] = pInfo

	key := "abcdef%d"
	value := "zbtrql%d"
	writesN := 1000000
	for i := 1; i <= writesN; i++ {
		ts := NanoTime()
		details_1, _ := pInfo.logWriter.Write([]byte(fmt.Sprintf(key, i)), []byte(fmt.Sprintf(value, i)), ts, 1)
		if details_1 != nil { //rollover took place
			pInfo.inactiveLogDetails = append(pInfo.inactiveLogDetails, details_1)
			break
		}
	}
	pInfo.logWriter.FlushAndClose()
	logDetails := pInfo.inactiveLogDetails[0]
	require.True(t, DefaultConstants.logFileMaxLen >= fileSize(logDetails.FileName))
}
func fileSize(name string) uint32 {
	fi, _ := os.Stat(name)
	return uint32(fi.Size())
}
