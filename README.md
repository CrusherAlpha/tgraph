# TGraph
## Intro
TGraph is a property evolution OLTP temporal graph database.
TGraph is based on Neo4j and RocksDB, and TGraph implements
read committed transaction on them. TGraph stores graph structure
on Neo4j and puts temporal properties values on RocksDB.

## TODO
- [x] project structure and interface design
- [ ] graph and temporal property store
- [ ] high efficient WAL
- [ ] pessimistic transaction based on property granularity lock
- [ ] optimistic transaction based on write/read set validation
- [ ] mvcc optimization
- [ ] two phase commit interface
- [ ] transaction feature test
- [ ] benchmark