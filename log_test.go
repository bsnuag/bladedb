package bladedb

import (
	"bladedb/memstore"
	"bytes"
	"fmt"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"testing"
)

func BenchmarkEncoder(b *testing.B) {
	ts := NanoTime()
	logRec1 := LogRecord{1, bytes.Repeat([]byte("1"), 65000), bytes.Repeat([]byte("2"), 65000), ts}
	buf := make([]byte, 3000000)
	for i := 0; i < b.N; i++ {
		logRec1.Encode(buf)
	}
}

func BenchmarkEncoderDecoder(b *testing.B) {
	ts := NanoTime()
	logRec1 := LogRecord{1, bytes.Repeat([]byte("1"), 65000), bytes.Repeat([]byte("2"), 65000), ts}
	buf := make([]byte, 3000000)
	for i := 0; i < b.N; i++ {
		var off uint32 = 0
		n := logRec1.Encode(buf)
		logRec2 := Decode(&off, buf[0:n])
		require.True(b, logRec1.equals(logRec2))
	}
}

func TestEncoderDecoder_ChainRecords(t *testing.T) {
	buf := make([]byte, 3000000)
	key := "abcdef%d"
	value := "abcdef%d"
	ts := NanoTime()

	logRec1 := LogRecord{1, []byte(fmt.Sprintf(key, 1)),
		[]byte(fmt.Sprintf(value, 1)), ts}

	logRec2 := LogRecord{1, []byte(fmt.Sprintf(key, 2)),
		[]byte(fmt.Sprintf(value, 2)), ts}
	encode := make([]byte, 100000)
	n1 := logRec1.Encode(buf)
	copy(encode[0:n1], buf[:n1])
	n2 := logRec2.Encode(buf)
	copy(encode[n1:n1+n2], buf[:n2])

	var off uint32 = 0
	logRec3 := Decode(&off, encode)
	logRec4 := Decode(&off, encode)

	require.True(t, logRec1.equals(logRec3))
	require.True(t, logRec2.equals(logRec4))
}

func TestParallelEncoderDecoder(t *testing.T) {
	var eachBufLen uint32 = 3000000

	buf := make([]byte, 4*eachBufLen)
	t.Run("OffsetStart=0", func(t *testing.T) {
		t.Parallel()
		testEncoderDecoder(t, bytes.Repeat([]byte("abcdefwd"), 1), bytes.Repeat([]byte("okijwdax"), 1), 1000000, 0*eachBufLen, eachBufLen, buf)
	})
	t.Run("OffsetStart=3000000", func(t *testing.T) {
		t.Parallel()
		testEncoderDecoder(t, bytes.Repeat([]byte("abc123xy"), 16), bytes.Repeat([]byte("xyz987po"), 16), 1000000, 1*eachBufLen, eachBufLen, buf)
	})
	t.Run("OffsetStart=6000000", func(t *testing.T) {
		t.Parallel()
		testEncoderDecoder(t, bytes.Repeat([]byte("sa_123qa"), 512), bytes.Repeat([]byte("13x_-987"), 512), 1000000, 2*eachBufLen, eachBufLen, buf)
	})
	t.Run("OffsetStart=9000000", func(t *testing.T) {
		t.Parallel()
		testEncoderDecoder(t, bytes.Repeat([]byte("a*c1*3x#"), 512), bytes.Repeat([]byte("13x#%987"), 512), 1000000, 3*eachBufLen, eachBufLen, buf)
	})
}

func testEncoderDecoder(t *testing.T, key []byte, value []byte, N int, offset uint32, BufLen uint32, buf []byte) {
	for i := 0; i < N; i++ {
		var off uint32 = 0
		ts := NanoTime()
		logRec1 := LogRecord{1, key, value, ts}
		n := logRec1.Encode(buf[offset : offset+BufLen])
		logRec2 := Decode(&off, buf[offset:offset+n])
		require.True(t, logRec1.equals(logRec2))
	}
}

func (logRec1 LogRecord) equals(logRec2 LogRecord) bool {
	return logRec1.recType == logRec2.recType &&
		bytes.Equal(logRec1.key, logRec2.key) &&
		bytes.Equal(logRec1.value, logRec2.value) &&
		logRec1.ts == logRec2.ts
}

func prepareLogTest(pNum int) (cFileName string, tearTest func()) {
	config := Config{LogFileMaxLen: DefaultLogFileMaxLen,
		LogDir:              "logTestLog/",
		DataDir:             "logTestSST/",
		WalFlushPeriodInSec: 10,
		CompactWorker:       0,
		NoOfPartitions:      pNum,
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

//Writes data only to log and recover it. Matches recovered data is same with old written data
func TestLogWrite_RecoverRead(t *testing.T) {
	cFileName, tearTest := prepareLogTest(2)
	defer tearTest()
	defer Drain()
	Open(cFileName)

	key := "abcdef%d"
	value := "zbtrql%d"
	writesN := 1000
	for _, pInfo := range db.pMap {
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
		oldMemTable := pInfo.memTable
		pInfo.memTable = memstore.NewMemStore()
		for _, logDetails := range pInfo.inactiveLogDetails {
			details, err := pInfo.loadLogFile(logDetails.FileSeqNum)
			require.Nil(t, err)
			require.NotNil(t, details.WriteOffset)
			fsz, _ := fileSize(logDetails.FileName)
			require.True(t, details.WriteOffset == uint32(fsz))
			require.True(t, (pInfo.memTable.Size() == int64(writesN/2)) || pInfo.memTable.Size() == int64(writesN))
			itr := oldMemTable.Recs().NewIterator()
			for itr.Next() {
				next := itr.Value()
				oldKey := []byte(next.Key())
				oldMemRec := next.Value().(*memstore.MemRec)
				activeMemRec, _ := pInfo.memTable.Find(oldKey)
				require.True(t, bytes.Equal(oldMemRec.Value, activeMemRec.Value))
				require.True(t, oldMemRec.TS == activeMemRec.TS)
				require.True(t, oldMemRec.RecType == activeMemRec.RecType)
			}
		}
		require.True(t, len(pInfo.inactiveLogDetails) == 2)
	}
}

//Writes data only to log and recover it.
func TestLogWrite_RecoverRead_WithOneEmptyFile(t *testing.T) {
	cFileName, tearTest := prepareLogTest(1)
	defer tearTest()
	defer Drain()
	Open(cFileName)

	partitionId := 0
	pInfo := db.pMap[partitionId]

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
	fsz, _ := fileSize(logDetails_1.FileName)
	require.True(t, details_1.WriteOffset == uint32(fsz))
	require.True(t, pInfo.memTable.Size() == int64(writesN))
	//
	emptyLogFileDetails := pInfo.inactiveLogDetails[1]
	details2, err := pInfo.loadLogFile(emptyLogFileDetails.FileSeqNum)
	require.Nil(t, details2)
	require.True(t, err == EmptyFile, "Error Message Should be EmptyFile")
}

func TestLogRollover(t *testing.T) {
	cFileName, tearTest := prepareLogTest(1)
	defer tearTest()
	defer Drain()
	Open(cFileName)

	partitionId := 0
	pInfo := NewPartition(partitionId)
	logWriter, _ := newLogWriter(partitionId, pInfo.getNextLogSeq())
	pInfo.logWriter = logWriter
	db.pMap[partitionId] = pInfo

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
	fsz, _ := fileSize(logDetails.FileName)
	require.True(t, db.config.LogFileMaxLen >= uint32(fsz))
}
