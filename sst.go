package bladedb

import (
	"bladedb/sklist"
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/niubaoshu/gotiny"
	"os"
)

type IndexRec struct {
	SSTRecOffset  uint32 //considering max sst file size 160MB = 1.6e+8 Bytes
	SSTFileSeqNum uint32 //sst files will have a seq num
	TS            int64  //TS - helpful for TTL, new write load
}

//TODO - next implement compaction
type SSTRec struct {
	key  []byte
	val  []byte
	meta SSTRecMeta
}

type SSTRecMeta struct {
	recType  byte //write or tombstomb
	ts       int64
	checksum int64 //check logic for checksum
}

var SST_HEADER_LEN uint32 = 5
//var SST_TABLE_MAX_LEN int64 = 1024
//var maxSSTRecLen = 1024 //TODO - decide a proper len

//var SSTFileFormat = "sst_%d_[0-9]+_[0-9]+\\.sst"

var SSTBaseFileName = "sst_%d_%d.sst"

//filename format - sst_partitionId_levelNo_sstSeqNum.sst TODO - make changes in all places
//one file for each partition

/*
	One SST for each partition hence partition Id
*/
type SSTWriter struct {
	file        *os.File
	writer      *bufio.Writer
	partitionId int    //which partition this sst belongs to
	SeqNum      uint32 // seqNum of sst file
	Offset      uint32
}

func (pInfo *PartitionInfo) NewSSTWriter() (*SSTWriter, error) {
	return NewSSTWriter(pInfo.partitionId, pInfo.getNextSSTSeq())
}

func NewSSTWriter(partitionId int, seqNum uint32) (*SSTWriter, error) {

	fileName := defaultConstants.SSTDir + fmt.Sprintf(SSTBaseFileName, partitionId, seqNum)
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		panic("Error while opening or creating sst file")
		return nil, err
	}

	sstWriter := &SSTWriter{
		file:        file,
		writer:      bufio.NewWriter(file),
		partitionId: partitionId,
		SeqNum:      seqNum,
	}
	return sstWriter, nil
}

func (writer *SSTWriter) Write(key []byte, value []byte, ts int64, recType byte) (byteWritten int16, err error) {
	sstMeta := SSTRecMeta{recType, ts, 0}

	sstRec := gotiny.Marshal(&SSTRec{key, value, sstMeta})
	var headerBuf = make([]byte, SST_HEADER_LEN)

	totalWriteLen := SST_HEADER_LEN + uint32(len(sstRec))
	//if data can't be written, flush and create a new file
	//if (writer.offset + totalWriteLen) > SST_TABLE_MAX_LEN {
	//	writer.FlushAndClose()
	//}
	binary.LittleEndian.PutUint32(headerBuf[0:SST_HEADER_LEN], uint32(len(sstRec)))

	if _, err := writer.writer.Write(headerBuf[:]); err != nil {
		return 0, err
	}
	//TODO - if len(sstRec) > maxSSTRecLen throw error
	if _, err := writer.writer.Write(sstRec); err != nil {
		return 0, err
	}
	writer.Offset = uint32(totalWriteLen) + writer.Offset
	return int16(totalWriteLen), nil
}

func (writer *SSTWriter) FlushAndClose() (uint32, error) {
	var oldSeqNum uint32 = 0
	if writer.writer != nil {
		if err := writer.writer.Flush(); err != nil {
			return oldSeqNum, err
		}
		if err := writer.file.Sync(); err != nil {
			return oldSeqNum, err
		}
		if err := writer.file.Close(); err != nil {
			return oldSeqNum, err
		}
	}
	oldSeqNum = writer.SeqNum
	//writer.SeqNum++

	//fileName := baseFileName + fmt.Sprintf("%d_%d", writer.seqNum, writer.partitionId) + fileExt
	//file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	//
	//if err != nil {
	//	panic("Error while opening or creating sst file")
	//	return err
	//}
	//
	//writer.file = file
	//writer.writer = bufio.NewWriter(file)
	//writer.offset = 0

	return oldSeqNum, nil
}

type SSTReader struct {
	file        *os.File
	SeqNm       uint32
	partitionId int

	//meta info
	startKey     []byte
	endKey       []byte
	noOfDelReq   uint64
	noOfWriteReq uint64
	//TODO - may want to add below props..
	//time it got created..
}

type SSTReaderMeta struct {
	startKey     []byte
	endKey       []byte
	noOfDelReq   uint64
	noOfWriteReq uint64
	//TODO - may want to add below props..
	//time it got created..
}

//TODO - this can be removed - improve code - design pattern + naming convention
func NewSSTReader(seqNum uint32, partitionId int) (SSTReader, error) {

	fileName := defaultConstants.SSTDir + fmt.Sprintf(SSTBaseFileName, partitionId, seqNum)
	fmt.Println(fmt.Sprintf("FileName for given seqNum: %d and partitionId: %d is %s", seqNum,
		partitionId, fileName))

	if _, err := os.Stat(fileName); err != nil {
		panic(fmt.Sprintf("Couldn't open sst file %s", fileName))
		return SSTReader{}, nil
	}
	return GetSSTReader(fileName, seqNum, partitionId)
}

func GetSSTReader(fileName string, sstFileSeqNum uint32, partitionId int) (SSTReader, error) {
	file, err := os.OpenFile(fileName, os.O_RDONLY, 0644)
	if err != nil {
		panic("Error while opening or creating sst file, fileName: " + fileName)
		return SSTReader{}, err
	}

	sstReader := SSTReader{
		file:         file,
		SeqNm:        sstFileSeqNum,
		partitionId:  partitionId,
		startKey:     nil,
		endKey:       nil,
		noOfDelReq:   0,
		noOfWriteReq: 0,
	}
	return sstReader, nil
}

//TODO - add try catch mechanism
func (reader SSTReader) ReadRec(offset int64) (*SSTRec, error) {
	reader.file.Seek(offset, 0) //TODO - whence- offset from which seek happens, add a check and seek from offset
	var lenRecBuf = make([]byte, SST_HEADER_LEN)
	reader.file.Read(lenRecBuf[0:SST_HEADER_LEN])
	fmt.Println(fmt.Sprintf("reading from offset: %d ", offset))
	fmt.Println(fmt.Sprintf("read from file: %v", lenRecBuf))

	var sstRecLength = binary.LittleEndian.Uint32(lenRecBuf[:])
	fmt.Println(fmt.Sprintf("sstRecLength from file: %v", sstRecLength))
	var sstRecBuf = make([]byte, sstRecLength)
	reader.file.Read(sstRecBuf[0:sstRecLength])

	var sstRec = &SSTRec{}
	gotiny.Unmarshal(sstRecBuf[:], sstRec)

	fmt.Println(fmt.Sprintf("sstRec from file, key: %v , value: %v , ts: %d",
		string(sstRec.key), string(sstRec.val), sstRec.meta.ts))
	return sstRec, nil
}

/*func exists(name string) (bool, error) {
	_, err := os.Stat(name)
	if os.IsNotExist(err) {
		return false, nil
	}
	return err != nil, err
}
*/

//bootstrapping activity
//index is thread-safe
func (reader *SSTReader) loadSSTRec(idx *sklist.SkipList) (int64, error) {
	var recsRead int64 = 0
	var readOffset uint32 = 0
	info, err := reader.file.Stat()
	if err != nil {
		panic(err)
		return recsRead, err
	}

	for readOffset < uint32(info.Size()) {
		sstRecLength, sstRec := reader.readNext()

		if sstRec.meta.recType == defaultConstants.writeReq {
			indexRec := &IndexRec{
				SSTRecOffset:  readOffset,
				SSTFileSeqNum: reader.SeqNm,
				TS:            sstRec.meta.ts,
			}

			//keyString := string(sstRec.key)
			keyHash, _ := GetHash(sstRec.key)

			indexVal := idx.Get(keyHash)
			if indexVal == nil {
				idx.Set(keyHash, indexRec)
			} else {
				idRec := indexVal.Value().(*IndexRec)
				if idRec.TS < sstRec.meta.ts {
					idx.Set(keyHash, indexVal)
				}
			}

			reader.noOfWriteReq++
		} else {
			reader.noOfDelReq++
		}

		if reader.startKey == nil {
			reader.startKey = sstRec.key
		}
		//TODO - startkey, endkey, noWrite, noDelete can be moved to SST file if this causes performance issue
		reader.endKey = sstRec.key
		readOffset += (SST_HEADER_LEN) + (sstRecLength)
		recsRead++
	}
	return recsRead, nil
}

func (reader SSTReader) readNext() (uint32, *SSTRec) {

	var sstHeaderBuf = make([]byte, SST_HEADER_LEN)
	reader.file.Read(sstHeaderBuf[0:SST_HEADER_LEN])
	var sstRecLength = binary.LittleEndian.Uint32(sstHeaderBuf[:])
	if sstRecLength == 0 {
		return 0, nil
	}
	var sstRecBuf = make([]byte, sstRecLength)
	reader.file.Read(sstRecBuf[0:sstRecLength])

	var sstRec = &SSTRec{}
	gotiny.Unmarshal(sstRecBuf[:], sstRec)

	return sstRecLength, sstRec
}

func (reader SSTReader) Close() error {
	fmt.Println(fmt.Sprintf("Closing SSTReader sst SeqNum: %d, PartitionId: %d", reader.SeqNm, reader.partitionId))
	return reader.file.Close()
}

//close sst and delete
func (reader SSTReader) closeAndDelete() {
	if err := reader.Close(); err != nil {
		panic(err)
	}
	if err := deleteSST(reader.partitionId, reader.SeqNm); err != nil {
		panic(err)
	}
}

func deleteSST(partitionId int, seqNum uint32) error { //TODO - if file doesn't exists then error ?
	fileName := defaultConstants.SSTDir + fmt.Sprintf(SSTBaseFileName, partitionId, seqNum)
	return os.Remove(fileName)
}
