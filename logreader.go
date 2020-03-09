package bladedb

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/niubaoshu/gotiny"
	"io"
	"os"
)

type LogReader struct {
	file       *os.File
	fileReader *bufio.Reader
}

func deleteLog(partitionId int, seqNum uint32) error {
	fileName := LogDir + fmt.Sprintf(LogBaseFileName, seqNum, partitionId)
	fmt.Println(fmt.Sprintf("delete log file: %s", fileName))
	return os.Remove(fileName)
}

func newLogReader(partitionId int, seqNum uint32) (*LogReader, error) {
	fileName := LogDir + fmt.Sprintf(LogBaseFileName, seqNum, partitionId)
	file, err := os.OpenFile(fileName, os.O_RDONLY, 0644)

	if err != nil {
		fmt.Printf("Error while opening WAL file:%s", fileName)
		return nil, err
	}
	return &LogReader{
		file:       file,
		fileReader: bufio.NewReader(file),
	}, nil
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
		if manifestRec.fop == defaultConstants.fileDelete {
			deleteLog(manifestRec.partitionId, manifestRec.seqNum) //delete log file in case it's not deleted
		} else {
			unclosedFiles[manifestRec.seqNum] = manifestRec
		}
	}

	fmt.Println(fmt.Sprintf("%d unclosed(undeleated) log files found", len(unclosedFiles)))
	for _, unClosedFile := range unclosedFiles {
		inactiveLogDetails, err := pInfo.loadLogFile(unClosedFile.seqNum)
		if err != nil {
			panic(err)
			return err
		}
		if inactiveLogDetails != nil {
			pInfo.handleRolledOverLogDetails(inactiveLogDetails)
		}
	}
	return nil
}

func (pInfo *PartitionInfo) loadLogFile(unClosedFileSeq uint32) (*InactiveLogDetails, error) {
	reader, err := newLogReader(pInfo.partitionId, unClosedFileSeq)
	if err != nil {
		panic(err)
		return nil, err
	}

	var recsRecovered = 0
	var recoveredBytes uint32 = 0

	for {
		var headerBuf = make([]byte, defaultConstants.logRecLen)
		_, err := reader.fileReader.Read(headerBuf[:])

		if err != nil {
			if err == io.EOF {
				break
			} else {
				return nil, err
			}
		}

		var recLength = binary.LittleEndian.Uint32(headerBuf[:])
		var recBuf = make([]byte, recLength)
		reader.fileReader.Read(recBuf[:])

		var walRec = LogRecord{}
		gotiny.Unmarshal(recBuf[:], &walRec)

		pInfo.memTable.Insert(walRec.Key(), walRec.Value(), walRec.header.ts, walRec.header.recType)

		recsRecovered++
		recoveredBytes += defaultConstants.logRecLen + recLength
	}

	if recoveredBytes == 0 {
		fmt.Printf("recovered 0 bytes from %s unclosed wal file, marking it for delete", reader.file.Name())
		return nil, nil
	}

	return &InactiveLogDetails{
		FileName:    reader.file.Name(),
		WriteOffset: recoveredBytes,
		PartitionId: pInfo.partitionId,
	}, nil
}
