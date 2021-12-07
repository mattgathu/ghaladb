
RoadMap 
--
* KV store features
* KV store interface
* Component traits
    - in-memory table
    - wal log
    - on-disk table
* Code the use-cases against API
* In-memory table implementation
* Write Ahead Log implementation
* On-disk table implementation
* In-memory table to on-disk table flushing
* On-disk tables compaction
* Data format
* Explore vectored IO and memory maps
* Alloy Model

### KV store features
* Open / closing a db
    - `new` used to open/create new db
    - db closed when it goes out of scope
        * need to implement finalizer code when db is `dropped`
* Read / write data to/from db
* Iterate kv pairs in db
* configuration
    * Support Async and Sync Write IO
    * options module to house all things parametrization
* error management
* batch writes
* Advanced features
    - atomic operations
    - db snapshots
    - skiplist memtable: 
        - https://github.com/crossbeam-rs/crossbeam/tree/master/crossbeam-skiplist



References
-- 
- https://arxiv.org/pdf/1812.07527.pdf LSM-based Storage Techniques: A Survey
- https://www.infoq.com/articles/API-Design-Joshua-Bloch/ Joshua Bloch: Bumper-Sticker API Design
- http://www.benstopford.com/2015/02/14/log-structured-merge-trees/ Log Structured Merge Trees
- https://www.quora.com/How-does-the-log-structured-merge-tree-work
- https://www.datastax.com/blog/leveled-compaction-apache-cassandra
- http://static.googleusercontent.com/media/research.google.com/en//archive/bigtable-osdi06.pdf
