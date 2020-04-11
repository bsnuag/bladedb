package bladedb

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/pkg/errors"
	"os"
	"syscall"
)

const sstEncoderBufMetaLen = 1<<8 - 1 //1 byte(recType) + 16 bytes(ts)
var sstBufLen = sstEncoderBufMetaLen + DefaultConstants.keyMaxLen + DefaultConstants.keyMaxLen

type SSTRec struct {
	recType byte
	key     []byte
	value   []byte
	ts      uint64
}

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

func (rec SSTRec) SSTEncoder(buf []byte) uint32 {
	binary.LittleEndian.PutUint16(buf[0:2], uint16(len(rec.key)))
	binary.LittleEndian.PutUint16(buf[2:4], uint16(len(rec.value)))
	binary.LittleEndian.PutUint16(buf[4:6], uint16(8))

	offset := 6
	buf[offset] = rec.recType
	offset += 1
	copy(buf[offset:], rec.key)
	offset += len(rec.key)
	copy(buf[offset:], rec.value)
	offset += len(rec.value)
	offset += binary.PutUvarint(buf[offset:], rec.ts)
	return uint32(offset)
}

func (writer *SSTWriter) Write(data []byte) (uint32, error) {
	n, err := writer.writer.Write(data[:])
	if err != nil {
		return 0, err
	}
	writer.Offset = uint32(n) + writer.Offset
	return uint32(n), nil
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
	data        []byte
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
	size := int(fileSize(fileName))
	data, err := syscall.Mmap(int(file.Fd()), 0, size, syscall.PROT_READ, syscall.MAP_PRIVATE)
	if err != nil {
		return SSTReader{}, errors.Wrapf(err, "Error while mmaping sst file: %s", fileName)
	}

	sstReader := SSTReader{
		data:         data,
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

func SSTDecoder(offset uint32, data []byte) (size uint32, sstRec SSTRec) {
	if offset >= uint32(len(data)) {
		return 0, SSTRec{}
	}
	size = offset

	keyLen := binary.LittleEndian.Uint16(data[offset : offset+2])
	offset += 2

	valueLen := binary.LittleEndian.Uint16(data[offset : offset+2])
	offset += 2

	tsLen := binary.LittleEndian.Uint16(data[offset : offset+2])
	offset += 2

	sstRec.recType = data[offset]
	offset += 1

	sstRec.key = data[offset : offset+uint32(keyLen)]
	offset += uint32(keyLen)

	sstRec.value = data[offset : offset+uint32(valueLen)]
	offset += uint32(valueLen)

	sstRec.ts, _ = binary.Uvarint(data[offset : offset+uint32(tsLen)])
	offset += uint32(tsLen)

	return offset - size, sstRec
}

//reading file shall go away with implementation of mmap files
func (reader SSTReader) ReadRec(offset uint32) (uint32, SSTRec) {
	return SSTDecoder(offset, reader.data)
}

//bootstrapping activity
//index is thread-safe
//this is invoked sequentially
func (reader *SSTReader) loadSSTRec(idx *Index) (uint32, error) {
	fmt.Println(fmt.Sprintf("loading sst seqno: %d, partitionId: %d", reader.SeqNm, reader.partitionId))
	var recsRead uint32 = 0
	var readOffset uint32 = 0
	iterator := reader.NewIterator()
	for {
		readOffset = iterator.offset
		sstRec, ok := iterator.Next()
		if !ok {
			break
		}
		keyHash := Hash(sstRec.key)
		idRec, ok := idx.Get(keyHash)
		if sstRec.recType == DefaultConstants.writeReq {
			indexRec := IndexRec{
				SSTRecOffset:  readOffset,
				SSTFileSeqNum: reader.SeqNm,
				TS:            sstRec.ts,
			}
			if !ok || idRec.TS < sstRec.ts {
				idx.Set(keyHash, indexRec)
			}
			reader.noOfWriteReq++
		} else {
			reader.noOfDelReq++
			//remove if delete timestamp is > any write timestamp
			if ok && idRec.TS < sstRec.ts {
				idx.Remove(keyHash)
			}
		}

		if reader.startKey == nil {
			reader.startKey = sstRec.key
		}
		reader.endKey = sstRec.key
		recsRead++
	}
	fmt.Println(fmt.Sprintf("loading complete "+
		"sst seqno: %d, partitionId: %d, recs loaded: %d, "+
		"delete-req: %d, write-req: %d, start-key: %s, end-key: %s",
		reader.SeqNm, reader.partitionId, recsRead, reader.noOfDelReq,
		reader.noOfWriteReq, string(reader.startKey), string(reader.endKey)))
	return recsRead, nil
}

func (reader SSTReader) Close() error {
	err := syscall.Munmap(reader.data)
	if err != nil {
		panic(errors.Wrapf(err, "error while un-mapping sst file seqNum: %d partitionId: %d", reader.SeqNm, reader.partitionId))
	}
	return reader.file.Close()
}

func deleteSST(partitionId int, seqNum uint32) error {
	fileName := SSTDir + fmt.Sprintf(SSTBaseFileName, partitionId, seqNum)
	return os.Remove(fileName)
}

type SSTIterator interface {
	Next() (sstRec SSTRec, ok bool)
}

func (iterator *IterableSST) Next() (sstRec SSTRec, ok bool) {
	size, rec := SSTDecoder(iterator.offset, iterator.data)
	if size == 0 {
		return rec, false
	}
	iterator.offset += size
	return rec, true
}

type IterableSST struct {
	mappedFileName string
	data           []byte
	offset         uint32
}

func (reader SSTReader) NewIterator() *IterableSST {
	return &IterableSST{reader.file.Name(), reader.data, 0}
}
