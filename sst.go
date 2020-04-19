package bladedb

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/pkg/errors"
	"os"
	"syscall"
	"time"
)

type SSTRec struct {
	recType byte
	key     []byte
	value   []byte
	ts      uint64
}

type SSTWriter struct {
	file        *os.File
	writer      *bufio.Writer
	partitionId int    //which partition this sst belongs to
	SeqNum      uint32 // seqNum of sst file

	startKey     []byte
	endKey       []byte
	noOfDelReq   uint32
	noOfWriteReq uint32
}

func (pInfo *PartitionInfo) NewSSTWriter() (*SSTWriter, error) {
	return NewSSTWriter(pInfo.partitionId, pInfo.getNextSSTSeq())
}

func NewSSTWriter(partitionId int, seqNum uint32) (*SSTWriter, error) {

	fileName := db.config.DataDir + fmt.Sprintf(DataFileFmt, partitionId, seqNum)
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)

	if err != nil {
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
	return uint32(n), nil
}

func (writer *SSTWriter) FlushAndClose() error {
	if writer.writer != nil {
		if err := writer.writer.Flush(); err != nil {
			return err
		}
		if err := writer.file.Sync(); err != nil {
			return err
		}
		if err := writer.file.Close(); err != nil {
			return err
		}
	}
	return nil
}

type SSTReader struct {
	data        []byte
	file        *os.File
	SeqNm       uint32
	partitionId int
	//meta info
	startKey     []byte
	endKey       []byte
	noOfDelReq   uint32
	noOfWriteReq uint32
}

func (writer *SSTWriter) NewSSTReader() (SSTReader, error) {
	reader, err := NewSSTReader(writer.SeqNum, writer.partitionId)
	if err != EmptyFile && err != nil {
		return SSTReader{}, errors.Wrapf(err, "error while adding new sst reader for seqnum: %d partitionId: %d",
			writer.SeqNum, writer.partitionId)
	}
	reader.startKey = writer.startKey
	reader.endKey = writer.endKey
	reader.noOfWriteReq = writer.noOfWriteReq
	reader.noOfDelReq = writer.noOfDelReq
	return reader, nil
}

func NewSSTReader(seqNum uint32, partitionId int) (SSTReader, error) {
	fileName := db.config.DataDir + fmt.Sprintf(DataFileFmt, partitionId, seqNum)
	size, err := fileSize(fileName)
	if err == EmptyFile {
		return SSTReader{}, EmptyFile
	} else if err != nil {
		return SSTReader{}, err
	}
	file, err := os.OpenFile(fileName, os.O_RDONLY, 0400) //TODO - can be moved to fileutils - a common place

	if err != nil {
		return SSTReader{}, errors.Wrapf(err, "Error while opening sst file: %s", fileName)
	}
	data, err := syscall.Mmap(int(file.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_PRIVATE)
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
	startT := time.Now()
	db.logger.Info().Msgf("loading sst file: %s, partition: %d", reader.file.Name(), reader.partitionId)
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
		if sstRec.recType == WriteReq {
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
	db.logger.Info().Msgf("sst loading complete "+
		"duration(sec): %f, file: %s, partitionId: %d, recs loaded: %d, "+
		"delete-req: %d, write-req: %d, start-key: %s, end-key: %s",
		time.Since(startT).Seconds(), reader.file.Name(), reader.partitionId, recsRead, reader.noOfDelReq,
		reader.noOfWriteReq, string(reader.startKey), string(reader.endKey))
	return recsRead, nil
}

func (reader SSTReader) Close() error {
	err := syscall.Munmap(reader.data)
	if err != nil {
		return errors.Wrapf(err, "error while un-mapping sst file seqNum: %d partitionId: %d", reader.SeqNm, reader.partitionId)
	}
	err = reader.file.Close()
	if err != nil {
		return errors.Wrapf(err, "error while closing sst file seqNum: %d partitionId: %d", reader.SeqNm, reader.partitionId)
	}
	return nil
}

func deleteSST(partitionId int, seqNum uint32) error {
	fileName := db.config.DataDir + fmt.Sprintf(DataFileFmt, partitionId, seqNum)
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
