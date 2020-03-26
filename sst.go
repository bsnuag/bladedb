package bladedb

import (
	"bladedb/index"
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/niubaoshu/gotiny"
	"github.com/pkg/errors"
	"os"
)

type SSTRec struct {
	key  []byte
	val  []byte
	meta SSTRecMeta
}

type SSTRecMeta struct {
	recType  byte //write or tombstone
	ts       int64
	checksum int64 //TODO - impl checksum
}

var SST_HEADER_LEN uint32 = 5
//var SST_TABLE_MAX_LEN int64 = 1024
//var maxSSTRecLen = 1024 //TODO - decide a proper len

var SSTDir = "data/dbstore"
var SSTBaseFileName = "/sst_%d_%d.sst"

type SSTWriter struct {
	file        *os.File
	writer      *bufio.Writer
	partitionId int    //which partition this sst belongs to
	SeqNum      uint32 // seqNum of sst file
	Offset      uint32

	startKey     []byte
	endKey       []byte
	noOfDelReq   uint64
	noOfWriteReq uint64
}

func (pInfo *PartitionInfo) NewSSTWriter() (*SSTWriter, error) {
	return NewSSTWriter(pInfo.partitionId, pInfo.getNextSSTSeq())
}

func NewSSTWriter(partitionId int, seqNum uint32) (*SSTWriter, error) {

	fileName := SSTDir + fmt.Sprintf(SSTBaseFileName, partitionId, seqNum)
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)

	if err != nil {
		panic("Error while opening or creating sst file")
		return nil, err
	}

	sstWriter := &SSTWriter{
		file:        file,
		writer:      bufio.NewWriterSize(file, 1000000), //TODO - take size as an arg
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
	binary.LittleEndian.PutUint32(headerBuf[0:SST_HEADER_LEN], uint32(len(sstRec)))

	if _, err := writer.writer.Write(headerBuf[:]); err != nil {
		return 0, err
	}
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
}

func NewSSTReader(seqNum uint32, partitionId int) (SSTReader, error) {
	fileName := SSTDir + fmt.Sprintf(SSTBaseFileName, partitionId, seqNum)
	file, err := os.OpenFile(fileName, os.O_RDONLY, 0400) //TODO - can be moved to fileutils - a common place

	if err != nil {
		return SSTReader{}, errors.Wrapf(err, "Error while opening sst file: %s", fileName)
	}
	sstReader := SSTReader{
		file:         file,
		SeqNm:        seqNum,
		partitionId:  partitionId,
		startKey:     nil,
		endKey:       nil,
		noOfDelReq:   0,
		noOfWriteReq: 0,
	}
	return sstReader, nil
}

func (reader SSTReader) ReadRec(offset int64) (*SSTRec, error) {
	var lenRecBuf = make([]byte, SST_HEADER_LEN)
	reader.file.ReadAt(lenRecBuf[0:SST_HEADER_LEN], offset)

	var sstRecLength = binary.LittleEndian.Uint32(lenRecBuf[:])
	if sstRecLength == 0 {
		return nil, nil
	}
	var sstRecBuf = make([]byte, sstRecLength)
	reader.file.ReadAt(sstRecBuf[0:sstRecLength], offset+int64(SST_HEADER_LEN))

	var sstRec = &SSTRec{}
	gotiny.Unmarshal(sstRecBuf[:], sstRec)

	return sstRec, nil
}

//bootstrapping activity
//index is thread-safe
//this is invoked sequentially
func (reader *SSTReader) loadSSTRec(idx *index.SkipList) (int64, error) {
	var recsRead int64 = 0
	var readOffset uint32 = 0
	info, err := reader.file.Stat()
	if err != nil {
		panic(err)
		return recsRead, err
	}

	for readOffset < uint32(info.Size()) {
		sstRecLength, sstRec := reader.readNext()

		if sstRec.meta.recType == DefaultConstants.writeReq {
			indexRec := index.IndexRec{
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
				idRec := indexVal.Value()
				if idRec.TS < sstRec.meta.ts {
					idx.Set(keyHash, idRec)
				}
			}

			reader.noOfWriteReq++
		} else {
			reader.noOfDelReq++
			keyHash, _ := GetHash(sstRec.key)
			indexVal := idx.Get(keyHash)
			//remove if delete timestamp is > any write timestamp
			if indexVal != nil {
				idRec := indexVal.Value()
				if idRec.TS < sstRec.meta.ts {
					idx.Remove(keyHash)
				}
			}
		}

		if reader.startKey == nil {
			reader.startKey = sstRec.key
		}
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
	return reader.file.Close()
}

func deleteSST(partitionId int, seqNum uint32) error {
	fileName := SSTDir + fmt.Sprintf(SSTBaseFileName, partitionId, seqNum)
	return os.Remove(fileName)
}
