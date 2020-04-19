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

func deleteLog(partitionId int, seqNum uint32) {
	fName := db.config.LogDir + fmt.Sprintf(LogFileFmt, seqNum, partitionId)
	if err := os.Remove(fName); err != nil && os.IsExist(err) {
		db.logger.Error().Err(err).Msgf("Error while deleting logfile: %s", fName)
	} else {
		db.logger.Info().Msgf("logfile: %s deleted", fName)
	}
}

func (pInfo *PartitionInfo) maxLogSeq(manifest *Manifest) uint32 {
	maxLogSeq := uint32(0)
	if manifest == nil {
		return maxLogSeq
	}

	for _, manifestRec := range manifest.logManifest[pInfo.partitionId].manifestRecs {
		if manifestRec.seqNum > maxLogSeq {
			maxLogSeq = manifestRec.seqNum
		}
	}
	return maxLogSeq
}

//log files which were not successfully written to SST
func (pInfo *PartitionInfo) loadUnclosedLogFile(manifest *Manifest) error {
	if manifest == nil {
		db.logger.Info().Msgf("no unclosed log files for partition: %d", pInfo.partitionId)
		return nil
	}
	unclosedFiles := make(map[uint32]ManifestRec)
	for _, manifestRec := range manifest.logManifest[pInfo.partitionId].manifestRecs {
		if manifestRec.fop == fDelete {
			deleteLog(manifestRec.partitionId, manifestRec.seqNum) //delete log file in case it's not deleted
		} else {
			unclosedFiles[manifestRec.seqNum] = manifestRec
		}
	}
	db.logger.Info().Msgf("%d unclosed log files found for partition: %d", len(unclosedFiles), pInfo.partitionId)
	for _, unClosedFile := range unclosedFiles {
		inactiveLogDetails, err := pInfo.loadLogFile(unClosedFile.seqNum)
		if err != EmptyFile && err != nil {
			return err
		}
		if inactiveLogDetails != nil {
			pInfo.handleRolledOverLogDetails(inactiveLogDetails)
		}
		if err == EmptyFile {
			mf1 := ManifestRec{
				partitionId: unClosedFile.partitionId,
				seqNum:      unClosedFile.seqNum,
				fop:         fDelete,
				fileType:    LogFileType,
			}
			writeManifest([]ManifestRec{mf1})
			deleteLog(unClosedFile.partitionId, unClosedFile.seqNum)
		}
	}
	return nil
}

func (pInfo *PartitionInfo) loadLogFile(unClosedFileSeq uint32) (*InactiveLogDetails, error) {
	logFile := db.config.LogDir + fmt.Sprintf(LogFileFmt, unClosedFileSeq, pInfo.partitionId)
	_, err := fileSize(logFile)
	if err != nil {
		return nil, err
	}
	logBytes, err := ioutil.ReadFile(logFile)
	if err != nil {
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
		pInfo.memTable.Insert(decodedLogRec.key, decodedLogRec.value, decodedLogRec.ts, decodedLogRec.recType)
		recsRecovered++
	}
	inactiveLogDetails.WriteOffset = offset
	if offset == 0 {
		db.logger.Info().Msgf("recovered 0 bytes from %s unclosed log file", logFile)
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
