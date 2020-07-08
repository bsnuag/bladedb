# BladeDB [![Go Report Card](https://goreportcard.com/badge/github.com/bsnuag/bladedb)](https://goreportcard.com/report/github.com/bsnuag/bladedb)

**BladeDB is a persistent, KV store written in go**

## Motivation

After learning distributed systems (especially databases), it's internals, i started writing BladeDB to give a shape to my learnings. The design and implementations are derived from many production ready databases & books.  

## Usage



## Implementation

Overview of BladeDB implementation - 

- Sharding: 
    1. By default BladeDB divides the KV into 64 (configurable) shards. 
    2. Each key (K) belong to one shard, decided using hash algorithm. 
    3. Each shard has it's own components (storage, index) isolated from others.   
- Data Distribution: 
    1. Distributes data by sharding, by design it supports distribution of shards across nodes. Implementation of shard replication or distribution is pending.
- Storage: 
    1. It uses LSM tree concept for storage. 
    2. WAL files are used for atomicity across writes. (Gets flushed periodically) 
    3. Data is written to SSTable once WAL reaches a threshold or when manual flush is triggered.
- Compaction: 
    1. Uses levelled compaction strategy.
    2. At most one compaction per shard. 
- Index: 
    1. BladeDB is designed for SSDs devices. 
    2. Index is used for faster retrieval, instead doing Binary Search across SSTables  
    3. An index is build for all active sstables when DB starts time
    4. The key (K) is hashed using sha-256 algorithm which generates 32 byte hashed data - Chances of [collision](https://crypto.stackexchange.com/questions/47809/why-havent-any-sha-256-collisions-been-found-yet) is very less  
    5. Each index entry takes about 44 bytes (32-hash + sstable-offset-details + timestamp). Roughly 22.7 Millions key cab be stored in 1 GB of memory 
- Thread Safety: 
    1. BladeDB is thread safe (uses RW lock), parallelism is defined by number of shards for write operations
    2. Lock is implemented per shard per record level.

## Future Work

- Making it Distributed: Use Raft consensus algorithm to make it distributed KV Store
- Scan: As on today it supports basic operations like Get, Put, Delete operations 
- Data Compression
- Backup & Recovery
- Monitoring Stats - Stats like Disk, Memory, CPU, Compactions  
- Optimisation: With initial implementation many possible optimisations were not done. Below are few:
    1. Memory mapped (mmap) file for WAL (currently it uses buffered file)
    2. Replacing value from in-memory sstable (data which is not flushed yet) with WAL file offset details - Performance evaluation is needed
    3. Reduce Disk Space utilization during compaction - A particular SSTable can be deleted immediately during compaction once processing completes for that file - Inspired by Scylla Hybrid compaction strategy       
    
    
## Configuration

It allows client to override some of config parameters - 

Parameter | Default value | Comment
--- | --- | ---
log-flush-interval | 10 sec | frequency in which wal file data gets flushed to disk. When value is too high - data loss chances increases, when too low - performance will degrade
partitions | 64 | number of shards
compact-worker | 8 | number of compaction can be executed in parallel (one compaction per shard)
memflush-worker | 8 | number of memflush can be executed in parallel (low value for write load use may increase memory usage) 

## Benchmarks