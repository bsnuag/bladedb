package bladedb

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/niubaoshu/gotiny"
	"io"
	"os"
)

/*
This is limited to add check-pointing to log file
TODO - This can be removed
*/

type LogReader struct {
	file       *os.File
	fileReader *bufio.Reader
}

/*func NewReader() (*LogReader, error) {
	var fileName = getNextFile("") //initially fileName is blank

	if len(fileName) == 0 {
		panic(fmt.Sprintf("No commit log files to read"))
		return nil, errors.New("no commit log files to read")
	}

	file, err := os.Open(fileName)
	if err != nil {
		panic(fmt.Sprintf("Error while opening commit log file %v", err))
		return nil, err
	}

	logReader := &LogReader{
		file:       file,
		fileReader: bufio.NewReader(file),
	}
	return logReader, nil
}

var dir = "commitlog/"
var logFileNames []string //remove it, always return next fileName by scanning directory

//can be optimised to not use listFiles() always
// when machine boots due to failure and recover needed, it needs to know the last log file
func getNextFile(currFileName string) string {
	listFiles()
	var in = -1
	for e := range logFileNames {
		if logFileNames[e] == currFileName {
			in = e
			break
		}
	}
	if in == len(logFileNames)-1 {
		return ""
	}
	return logFileNames[in+1]
}*/

/*func listFiles() {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		log.Fatal(err)
	}
	logFileNames = nil
	var in = 0
	for _, f := range files {
		matched, _ := regexp.Match(`log_[0-9]{19}.*log`, []byte(f.Name()))
		if matched {
			logFileNames[in] = f.Name()
			in++
		}
	}
	sort.Strings(logFileNames)
}*/

func deleteLog(partitionId int, seqNum uint32) error {
	fileName := baseFileName + fmt.Sprintf("%d_%d", seqNum, partitionId) + fileExt
	return os.Remove(fileName)
}

func newLogReader(partitionId int, seqNum uint32) (*LogReader, error) {
	fileName := baseFileName + fmt.Sprintf("%d_%d", seqNum, partitionId) + fileExt
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
		fmt.Println("no unclosed wal files")
		return nil
	}
	unclosedFiles := make(map[uint32]ManifestRec)
	for _, manifestRec := range manifestFile.manifest.logManifest[pInfo.partitionId].manifestRecs {
		if manifestRec.fop == defaultConstants.fileDelete {
			if _, ok := unclosedFiles[manifestRec.seqNum]; !ok {
				panic("deleted wal file found but not create file in manifest")
			} else {
				delete(unclosedFiles, manifestRec.seqNum)
				deleteLog(manifestRec.partitionId, manifestRec.seqNum) //delete log file in case it's not deleted
			}
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
