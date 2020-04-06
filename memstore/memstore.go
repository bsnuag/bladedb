package memstore

import "bladedb/sklist"

/*
	Memtable per partition
	MemTable is implemented using hashmap which internally has bucketed lock
*/

type MemTable struct {
	list *sklist.SkipList
}

type MemRec struct {
	Value   []byte
	TS      uint64
	RecType byte
}

func NewMemStore() (*MemTable, error) {
	memTable := &MemTable{
		list: sklist.New(),
	}
	return memTable, nil
}

func (memTable *MemTable) Insert(key []byte, value []byte, ts uint64, reqType byte) {
	memTable.list.Set(string(key), &MemRec{value, ts, reqType})
}

func (memTable *MemTable) Find(key []byte) (value *MemRec, err error) {
	keyString := string(key)

	rec := memTable.list.Get(keyString)
	if rec != nil {
		return rec.Value().(*MemRec), nil
	}
	return nil, nil
}

func (memTable *MemTable) FindKeyString(key string) (value *MemRec, err error) {
	return memTable.Find([]byte(key))
}
func (memTable *MemTable) RefreshMemTable() (*sklist.SkipList) {
	oldList := memTable.list
	memTable.list = sklist.New()
	return oldList
}

func (memTable *MemTable) Recs() (*sklist.SkipList) {
	return memTable.list
}

func (memTable *MemTable) Size() (int64) {
	return int64(memTable.list.Length)
}
