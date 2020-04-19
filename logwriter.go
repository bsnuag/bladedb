package bladedb

//commit log will always have 2 files in active mode. 1. Actual log file  2. Log Stats file i.e. checkpoint_info, if file can be cleaned etc

import (
	"bladedb/memstore"
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
)

//LogRecord - record to be inserted into log-commit files
type LogRecord struct {
	recType byte //write or tombstone
	key     []byte
	value   []byte
	ts      uint64
}

//	LogWriter - writes record to log-commit file
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
	fileName := db.config.LogDir + fmt.Sprintf(LogFileFmt, seqNum, partitionId)
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)

	if err != nil {
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
		fop:         fCreate,
		fileType:    LogFileType,
	}
	writeManifest([]ManifestRec{mf1})
	return logWriter, nil
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
	offset += binary.PutUvarint(logRecBuf[offset:], logRec.ts)
	return uint32(offset)
}

//writes data to wal file
//checks if size is more than logFileMaxLen. closed it and returns a summary rec
func (writer *LogWriter) Write(key []byte, value []byte, ts uint64, recType byte) (*InactiveLogDetails, error) {
	offset := uint32(writer.partitionId) * LogEncoderPartLen //move to LogWriter
	logRec := LogRecord{recType, key, value, ts}
	n := logRec.Encode(db.logEncoderBuf[offset : offset+LogEncoderPartLen])

	var inactiveLogDetails *InactiveLogDetails = nil

	if writer.writeOffset+n >= db.config.LogFileMaxLen {
		logDetails, err := writer.rollover()
		if err != nil {
			return inactiveLogDetails, err
		}
		inactiveLogDetails = logDetails
	}

	if _, err := writer.fileWriter.Write(db.logEncoderBuf[offset : offset+n]); err != nil {
		return inactiveLogDetails, err
	}
	writer.writeOffset += n

	return inactiveLogDetails, nil
}

//flush & closes current wal file and assigns a new file to log writer
func (writer *LogWriter) rollover() (*InactiveLogDetails, error) {
	pInfo := db.pMap[writer.partitionId]
	newLogWriter, err := newLogWriter(writer.partitionId, pInfo.getNextLogSeq())
	if err != nil {
		return nil, err
	}
	db.logger.Info().Msgf("rolling log from: %v to: %v", writer.file.Name(), newLogWriter.LogFileName())
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

//Flush, Sync, Close
func (writer *LogWriter) FlushAndClose() (*InactiveLogDetails, error) {
	var inactiveLogDetails *InactiveLogDetails = nil

	//flush content of buffer to disk and close file
	if writer.fileWriter != nil {
		if err := writer.FlushAndSync(); err != nil {
			return inactiveLogDetails, err
		}

		if err := writer.file.Close(); err != nil {
			return inactiveLogDetails, err
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
