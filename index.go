package bladedb

import "sync"

func NewIndex() *Index {
	return &Index{
		lock:  &sync.RWMutex{},
		index: make(map[IndexKeyType]IndexRec, 10),
	}
}

type IndexKeyType [32]byte
type Index struct {
	lock  *sync.RWMutex
	index map[IndexKeyType]IndexRec
}

type IndexRec struct {
	SSTRecOffset  uint32 //considering max sst file size 160MB = 1.6e+8 Bytes
	SSTFileSeqNum uint32 //sst files will have a seq num
	TS            uint64 //TS - helpful for TTL
}

func (index *Index) Set(key IndexKeyType, value IndexRec) {
	index.lock.Lock()
	defer index.lock.Unlock()
	rec, ok := index.index[key]
	if !ok {
		index.index[key] = value
	} else {
		if value.TS > rec.TS {
			index.index[key] = value
		}
	}
}

func (index *Index) Get(key IndexKeyType) (rec IndexRec, ok bool) {
	index.lock.RLock()
	defer index.lock.RUnlock()
	rec, ok = index.index[key]
	return rec, ok
}

func (index *Index) Remove(key IndexKeyType) (rec IndexRec, ok bool) {
	index.lock.Lock()
	defer index.lock.Unlock()

	rec, ok = index.index[key]
	if ok {
		delete(index.index, key)
	}
	return rec, ok
}

func (index *Index) Size() int {
	return len(index.index)
}
