package bladedb

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
)

type LogReader struct {
	file       *os.File
	fileReader *bufio.Reader
}

func deleteLog(partitionId int, seqNum uint32) error {
	fileName := LogDir + fmt.Sprintf(LogBaseFileName, seqNum, partitionId)
	//fmt.Println(fmt.Sprintf("delete log file: %s", fileName))
	return os.Remove(fileName)
}

func maxLogSeq(partId int) (uint32, error) {
	maxLogSeq := uint32(0)
	if manifestFile.manifest == nil {
		fmt.Println("no manifest info to be loaded")
		return maxLogSeq, nil
	}

	for _, manifestRec := range manifestFile.manifest.logManifest[partId].manifestRecs {
		if manifestRec.seqNum > maxLogSeq {
			maxLogSeq = manifestRec.seqNum
		}
	}
	return maxLogSeq, nil
}

//log files which were not successfully written to SST
func (pInfo *PartitionInfo) loadUnclosedLogFile() error {
	if manifestFile.manifest == nil {
		fmt.Println("no unclosed wal files for partition: ", pInfo.partitionId)
		return nil
	}
	unclosedFiles := make(map[uint32]ManifestRec)
	for _, manifestRec := range manifestFile.manifest.logManifest[pInfo.partitionId].manifestRecs {
		if manifestRec.fop == DefaultConstants.fileDelete {
			deleteLog(manifestRec.partitionId, manifestRec.seqNum) //delete log file in case it's not deleted
		} else {
			unclosedFiles[manifestRec.seqNum] = manifestRec
		}
	}

	fmt.Println(fmt.Sprintf("%d unclosed(undeleated) log files found", len(unclosedFiles)))
	for _, unClosedFile := range unclosedFiles {
		inactiveLogDetails, err := pInfo.loadLogFile(unClosedFile.seqNum)
		if err != EmptyFile && err != nil {
			panic(err)
			return err
		}
		if inactiveLogDetails != nil {
			pInfo.handleRolledOverLogDetails(inactiveLogDetails)
		}
		if err == EmptyFile {
			mf1 := ManifestRec{
				partitionId: unClosedFile.partitionId,
				seqNum:      unClosedFile.seqNum,
				fop:         DefaultConstants.fileDelete,
				fileType:    DefaultConstants.logFileType,
			}
			writeManifest([]ManifestRec{mf1})
			deleteLog(unClosedFile.partitionId, unClosedFile.seqNum)
		}
	}
	return nil
}

func (pInfo *PartitionInfo) loadLogFile(unClosedFileSeq uint32) (*InactiveLogDetails, error) {
	logFile := LogDir + fmt.Sprintf(LogBaseFileName, unClosedFileSeq, pInfo.partitionId)
	size := fileSize(logFile)
	if size == 0 {
		return nil, EmptyFile
	}
	logBytes, err := ioutil.ReadFile(logFile)
	if err != nil {
		panic(err)
		return nil, err
	}

	var recsRecovered = 0
	var offset uint32 = 0

	inactiveLogDetails := &InactiveLogDetails{
		FileName:    logFile,
		WriteOffset: offset,
		PartitionId: pInfo.partitionId,
	}

	for {
		if offset == uint32(len(logBytes)) {
			break
		}

		decodedLogRec := Decode(&offset, logBytes)
		pInfo.memTable.Insert(decodedLogRec.Key(), decodedLogRec.Value(), decodedLogRec.ts, decodedLogRec.recType)
		recsRecovered++
	}
	inactiveLogDetails.WriteOffset = offset
	if offset == 0 {
		fmt.Printf("recovered 0 bytes from %s unclosed wal file, marking it for delete", logFile)
		return inactiveLogDetails, nil
	}

	return inactiveLogDetails, nil
}

type Decoder interface {
	Decode(recHeader []byte, recBuf []byte) LogRecord
}

func Decode(offset *uint32, logBuf []byte) (logRec LogRecord) {
	keyLen := binary.LittleEndian.Uint16(logBuf[*offset : *offset+2])
	*offset += 2
	valueLen := binary.LittleEndian.Uint16(logBuf[*offset : *offset+2])
	*offset += 2
	tsLen := binary.LittleEndian.Uint16(logBuf[*offset : *offset+2])
	*offset += 2
	logRec.recType = logBuf[*offset]
	*offset += 1
	logRec.key = logBuf[*offset : *offset+uint32(keyLen)]

	*offset += uint32(keyLen)
	logRec.value = logBuf[*offset : *offset+uint32(valueLen)]

	*offset += uint32(valueLen)
	logRec.ts, _ = binary.Uvarint(logBuf[*offset : *offset+uint32(tsLen)])
	*offset += uint32(tsLen)
	return logRec
}
