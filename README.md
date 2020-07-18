# BladeDB [![Go Report Card](https://goreportcard.com/badge/github.com/bsnuag/bladedb)](https://goreportcard.com/report/github.com/bsnuag/bladedb)

**BladeDB is a persistent, KV store written in go**

## Motivation

After learning distributed systems (especially databases), it's internals, i started writing BladeDB to give a shape to my learning. The design and implementations are derived from many production ready databases & books.  

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
    4. For write it uses buffered files (WAL and SST) and for read it memory maps SST files. (Avoids GC overhead)
Cache:
    1. BladeDB relies on OS and uses page cache 
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
- Data Compression & Checksum
- Backup & Recovery
- Monitoring Stats - Stats like Disk, Memory, CPU, Compactions
- A tool like nodetool (used in cassandra) to start, stop, get stats etc.   
- Optimisation: With initial implementation many possible optimisations were not done. Below are few:
    1. Replace in-memory skiplist (used to store active mem tables) with off-heap data structure to avoid GC cycles   
    2. Moving in-memory Index to Off-Heap to reduce effects of GC (Stop the world event) - Low RAM usage, would work with less RAM 
    3. Replacing value from in-memory sstable (data which is not flushed yet) with WAL file offset details - Performance evaluation is needed
    4. Reduce Disk Space utilization during compaction - A particular SSTable can be deleted immediately during compaction once processing completes for that file - Inspired by Scylla Hybrid compaction strategy
    5. Controller for memflush & compaction workers to avoid increase in write latency.  
    
## Configuration

It allows client to override some of config parameters - 

Parameter | Default value | Comment
--- | --- | ---
log-flush-interval | 10 sec | frequency in which wal file data gets flushed to disk. When value is too high - data loss chances increases, when too low - performance will degrade
partitions | 64 | number of shards
compact-worker | 8 | number of compaction can be executed in parallel (one compaction per shard)
memflush-worker | 8 | number of memflush can be executed in parallel (low value for write load use may increase memory usage) 

## Benchmarks

- it comes with inbuilt tool to perform benchmark with benchmark type, custom data size, parallelism
- Get all options to run benchmark tool -  
```
go run example/benchmark/benchEmbedded.go --help
  -kSz int
    	key size in bytes (default 256)
  -nR int
    	number of reads (default 40000000)
  -nThreads int
    	number of clients (default 8)
  -nW int
    	number of writes (default 40000000)
  -r	simulate read only
  -rw
    	simulate read write concurrently
  -vSz int
    	value size in bytes (default 256)
  -w	simulate write only
```  
- For -rw type benchmark, nThreads are divided equally and it doesn't wait for write to complete before read starts
- For -r read, data need to be ingested before

Config | Bench-Time(S)
--------------|--------------
``go run example/benchmark/benchEmbedded.go -nThreads=8 -nW=40000000 -nR=40000000 -w -kSz=256 -vSz=256`` | 185
``go run example/benchmark/benchEmbedded.go -nThreads=16 -nW=40000000 -nR=40000000 -r -kSz=256 -vSz=256`` | 115
``go run example/benchmark/benchEmbedded.go -nThreads=8 -nW=40000000 -nR=40000000 -w -kSz=512 -vSz=512`` | 338
``go run example/benchmark/benchEmbedded.go -nThreads=16 -nW=40000000 -nR=40000000 -r -kSz=512 -vSz=512`` | 205
``go run example/benchmark/benchEmbedded.go -nThreads=8 -nW=40000000 -nR=40000000 -w -kSz=1024 -vSz=1024`` | 605
``go run example/benchmark/benchEmbedded.go -nThreads=16 -nW=40000000 -nR=40000000 -r -kSz=1024 -vSz=1024`` | 390


Note: 
- Benchmark completes by flushing all active mem tables to disk, any possible compaction
- Above benchmark measurement are from Macbook pro configured with `16 GB 1600 MHz DDR3 RAM`, `2.2 GHz Quad-Core Intel Core i7 processor` and  `4 cores with hyper threading enabled`
- Benchmark DB Config 
```
data-dir: /tmp/bladedb.data/
log-dir: /tmp/bladedb.log/
log-flush-interval: 10
partitions: 8
compact-worker: 2
memflush-worker: 2
log-level: info
client-listen-port: 9099
``` 
   