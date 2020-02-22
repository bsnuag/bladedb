package memstore

import "bladedb/sklist"

/*
	Memtable per partition
	MemTable is implemented using hashmap which internally has bucketed lock
*/

//TODO - Could be replaced with skiplist for better performance
type MemTable struct {
	list        *sklist.SkipList
	partitionId int
	size        int64 //no of elements it holds
}

type MemRec struct {
	Key     []byte
	Value   []byte
	TS      int64
	RecType byte
}

func NewMemStore(partitionId int) (*MemTable, error) {
	memTable := &MemTable{
		list:        sklist.New(),
		partitionId: partitionId,
	}
	return memTable, nil
}

//TODO - check if we need this
//func (memTable *MemTable) Remove(key []byte) {
//	keyString := string(key)
//	memTable.list.Remove(keyString)
//}

func (memTable *MemTable) Insert(key []byte, value []byte, ts int64, reqType byte) {
	keyString := string(key)

	memTable.list.Set(keyString, &MemRec{key, value, ts, reqType})
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
	memTable.size = 0
	return oldList
}

func (memTable *MemTable) Recs() (*sklist.SkipList) {
	return memTable.list
}

func (memTable *MemTable) Size() (int64) {
	return int64(memTable.list.Length)
}
