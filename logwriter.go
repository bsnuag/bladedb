package bladedb

//commit log will always have 2 files in active mode. 1. Actual log file  2. Log Stats file i.e. checkpoint_info, if file can be cleaned etc

import (
	"bladedb/memstore"
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
)

var LogDir = "data/commitlog"
var LogBaseFileName = "/log_%d_%d.log"

const logEncoderBufMetaLen = 1<<8 - 1 //1 byte(recType) + 16 bytes(ts)

//logEncoderBuf - is shared among partitions, each works within a range of offset-offset+logEncoderPartLen. offset = partId * logEncoderPartLen
var logEncoderPartLen = logEncoderBufMetaLen + DefaultConstants.keyMaxLen + DefaultConstants.keyMaxLen
var logEncoderBuf = make([]byte, uint32(DefaultConstants.noOfPartitions)*logEncoderPartLen)

/*
	LogRecord - record to be inserted into log-commit files
*/
type LogRecord struct {
	recType byte //write or tombstone
	key     []byte
	value   []byte
	ts      uint64
}

func (record LogRecord) KeyString() string {
	return string(record.key)
}

func (record LogRecord) ValueString() string {
	return string(record.value)
}

func (record LogRecord) Key() []byte {
	return record.key
}

func (record LogRecord) Value() []byte {
	return record.value
}

func (record LogRecord) Ts() uint64 {
	return record.ts
}

/*
	LogWriter - writes record to log-commit file
*/
type LogWriter struct {
	file        *os.File
	fileWriter  *bufio.Writer
	writeOffset uint32 //offset till what data is written, contains bytes written
	partitionId int    //which partition this commit - log belongs to
	seqNum      uint32 // seqNum of log file
}

type InactiveLogDetails struct {
	FileName    string
	FileSeqNum  uint32
	WriteOffset uint32
	MemTable    *memstore.MemTable
	PartitionId int
}

func newLogWriter(partitionId int, seqNum uint32) (*LogWriter, error) {
	fileName := LogDir + fmt.Sprintf(LogBaseFileName, seqNum, partitionId)
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)

	if err != nil {
		fmt.Printf("Error while opening or creating WAL file:%s", fileName)
		return nil, err
	}

	logWriter := &LogWriter{
		file:        file,
		fileWriter:  bufio.NewWriter(file),
		writeOffset: 0,
		partitionId: partitionId,
		seqNum:      seqNum,
	}

	mf1 := ManifestRec{
		partitionId: partitionId,
		seqNum:      seqNum,
		fop:         DefaultConstants.fileCreate,
		fileType:    DefaultConstants.logFileType,
	}
	writeManifest([]ManifestRec{mf1})
	return logWriter, nil
}

type LogEncoder interface {
	Encode() ([]byte, []byte)
}

//6 byte logrec fields length, rest bytes data
func (logRec LogRecord) Encode(logRecBuf []byte) uint32 {
	binary.LittleEndian.PutUint16(logRecBuf[0:2], uint16(len(logRec.key)))
	binary.LittleEndian.PutUint16(logRecBuf[2:4], uint16(len(logRec.value)))
	binary.LittleEndian.PutUint16(logRecBuf[4:6], uint16(8))

	offset := 6
	logRecBuf[offset] = logRec.recType
	offset += 1
	copy(logRecBuf[offset:], logRec.key)
	offset += len(logRec.key)
	copy(logRecBuf[offset:], logRec.value)
	offset += len(logRec.value)
	offset += binary.PutUvarint(logRecBuf[offset:], logRec.Ts())
	return uint32(offset)
}

//writes data to wal file
//checks if size is more than LogFileMaxLen. closed it and returns a summary rec
func (writer *LogWriter) Write(key []byte, value []byte, ts uint64, recType byte) (*InactiveLogDetails, error) {
	offset := uint32(writer.partitionId) * logEncoderPartLen //move to LogWriter
	logRec := LogRecord{recType, key, value, ts}
	n := logRec.Encode(logEncoderBuf[offset : offset+logEncoderPartLen])

	var inactiveLogDetails *InactiveLogDetails = nil

	if writer.writeOffset+n >= DefaultConstants.logFileMaxLen {
		logDetails, err := writer.rollover()
		if err != nil {
			return inactiveLogDetails, err
		}
		inactiveLogDetails = logDetails
	}

	if _, err := writer.fileWriter.Write(logEncoderBuf[offset : offset+n]); err != nil {
		return inactiveLogDetails, err
	}
	writer.writeOffset += n

	return inactiveLogDetails, nil
}

//flush & closes current wal file and assigns a new file to wal writer
func (writer *LogWriter) rollover() (*InactiveLogDetails, error) {
	pInfo := partitionInfoMap[writer.partitionId]
	newLogWriter, err := newLogWriter(writer.partitionId, pInfo.getNextLogSeq())
	if err != nil {
		panic(err)
	}
	fmt.Println(LogRollingMsg, writer.file.Name(), newLogWriter.LogFileName())
	//flush content of buffer to disk and close file
	inactiveLogDetails, err := writer.FlushAndClose()

	if err != nil {
		return inactiveLogDetails, err
	}

	writer.file = newLogWriter.file
	writer.fileWriter = newLogWriter.fileWriter
	writer.seqNum = newLogWriter.seqNum
	writer.writeOffset = 0

	return inactiveLogDetails, nil
}

// Does Follwoing -
// 1. Closes running wal file & assigns new wal file
// 2. Return running wal file details i.e. InactiveLogDetails

func (writer *LogWriter) FlushAndRollOver() (*InactiveLogDetails, error) {
	return writer.rollover()
}

//Flush, Sync, Close
func (writer *LogWriter) FlushAndClose() (*InactiveLogDetails, error) {
	var inactiveLogDetails *InactiveLogDetails = nil

	//flush content of buffer to disk and close file
	if writer.fileWriter != nil {
		if err := writer.FlushAndSync(); err != nil {
			return inactiveLogDetails, nil
		}

		if err := writer.file.Close(); err != nil {
			return inactiveLogDetails, nil
		}
		inactiveLogDetails = &InactiveLogDetails{
			FileName:    writer.file.Name(),
			FileSeqNum:  writer.seqNum,
			WriteOffset: writer.writeOffset,
			PartitionId: writer.partitionId,
		}
	}
	return inactiveLogDetails, nil
}

func (writer *LogWriter) FlushAndSync() error {

	if err := writer.fileWriter.Flush(); err != nil {
		return err
	}

	if err := writer.file.Sync(); err != nil {
		return err
	}
	return nil
}

func (writer *LogWriter) LogFileName() string {
	return writer.file.Name()
}
