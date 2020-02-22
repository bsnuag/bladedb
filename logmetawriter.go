package bladedb

/*
/*package bladedb

import (
	"encoding/binary"
	"fmt"
	"github.com/niubaoshu/gotiny"
	"os"
	"time"
)

//CheckpointRecord - keeps a track of log fies like closed files, open files, check-point offset details for commit file
type CheckpointRecord struct {
	partId           int
	logFileName      string //can it be changed to partitionId and seq num
	checkPointOffset uint32 // offset till what data is persisted into sst
	fileStatus       byte   //1-file can be deleted
	ts               int64
}

type LogStatWriter struct {
	file *os.File
}

var statsFileBaseName = "data/commitlog/%d_stats.log"
//TODO - replace this stats file with less content when reaches certain size

//this is common across partitions
func NewLogStatsWriter(partitionId int) (*LogStatWriter, error) {
	statFile, err := os.OpenFile(fmt.Sprintf(statsFileBaseName, partitionId), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic("Error while opening or creating commit stats file")
		return nil, err
	}
	logStatWriter := &LogStatWriter{
		file: statFile,
	}
	return logStatWriter, nil
}

/*
+------------------+------------------------+
| 4byte rec-len    |        data		    |
+------------------+------------------------+
checkpoint is written to a stat file which holds rec of CheckpointRecord
*/

/*func (writer *LogStatWriter) AddCheckpoint(partId int, fileName string, offset uint32, fileStatus byte) error {
	rec := CheckpointRecord{
		partId:           partId,
		logFileName:      fileName,
		checkPointOffset: offset,
		fileStatus:       fileStatus,
		ts:               time.Now().Unix(),
	}
	fmt.Printf("addding stat to logstat: %v\n",rec)
	var recLen = make([]byte, defaultConstants.logStatRecLen)

	checkpointData := gotiny.Marshal(&rec)
	binary.LittleEndian.PutUint16(recLen[:], uint16(len(checkpointData)))

	if _, err := writer.file.Write(recLen[:]); err != nil {
		return err
	}

	if _, err := writer.file.Write(checkpointData); err != nil {
		return err
	}

	if err := writer.file.Sync(); err != nil {
		return err
	}

	return nil
}

func (writer *LogStatWriter) Close() error {
	if err := writer.file.Sync(); err != nil {
		return err
	}

	if err := writer.file.Close(); err != nil {
		return err
	}

	return nil
}
*/
