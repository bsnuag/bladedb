package bladedb

//commit log will always have 2 files in active mode. 1. Actual log file  2. Log Stats file i.e. checkpoint_info, if file can be cleaned etc

import (
	"bladedb/memstore"
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/niubaoshu/gotiny"
	"os"
)

var LogDir = "data/commitlog"
var LogBaseFileName = "/log_%d_%d.log"

/*
	LogRecord - record to be inserted into log-commit files
*/
type LogRecord struct {
	key    []byte
	value  []byte
	header Header
}

type Header struct {
	recType  byte //write or tombstone
	ts       int64
	checksum int64 //check logic for checksum
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

func (record LogRecord) Ts() (int64) {
	return record.header.ts
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
	return logWriter, nil
}

//writes data to wal file
//checks if size is more than LogFileMaxLen. closed it and returns a summary rec
func (writer *LogWriter) Write(key []byte, value []byte, ts int64, recType byte) (*InactiveLogDetails, error) {
	logHeader := Header{recType, ts, 0}
	logRec := LogRecord{key, value, logHeader}

	logRecByte := gotiny.Marshal(&logRec)

	logRecLenBuf := make([]byte, DefaultConstants.logRecLen)

	binary.LittleEndian.PutUint32(logRecLenBuf[:], uint32(len(logRecByte)))

	totalWriteLen := DefaultConstants.logRecLen + uint32(len(logRecByte))

	var inactiveLogDetails *InactiveLogDetails = nil

	if writer.writeOffset+totalWriteLen >= DefaultConstants.logFileMaxLen {
		logDetails, err := writer.rollover()
		if err != nil {
			return inactiveLogDetails, err
		}
		inactiveLogDetails = logDetails
	}

	if _, err := writer.fileWriter.Write(logRecLenBuf[:]); err != nil {
		return inactiveLogDetails, err
	}

	if _, err := writer.fileWriter.Write(logRecByte); err != nil {
		return inactiveLogDetails, err
	}
	writer.writeOffset += totalWriteLen

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

	mf1 := ManifestRec{
		partitionId: pInfo.partitionId,
		seqNum:      newLogWriter.seqNum,
		fop:         DefaultConstants.fileCreate,
		fileType:    DefaultConstants.logFileType,
	}
	writeManifest([]ManifestRec{mf1})
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
