```
 _____                                           ____________ 
/  ___|                                          |  _  \ ___ \
\ `--. _   _ _ __ ___  _ __ ___   __ _ _ __ _   _| | | | |_/ /
 `--. \ | | | '_ ` _ \| '_ ` _ \ / _` | '__| | | | | | | ___ \
/\__/ / |_| | | | | | | | | | | | (_| | |  | |_| | |/ /| |_/ /
\____/ \__,_|_| |_| |_|_| |_| |_|\__,_|_|   \__, |___/ \____/ 
                                             __/ |            
                                            |___/             
```                                            
![Go](https://github.com/Squadrick/summarydb/workflows/Go/badge.svg?branch=master)

This is an implementation of [SummaryStore](http://pages.cs.wisc.edu/~nitina/Publications/summarystore-sosp17.pdf)
in Golang.

By using window sliding aggregations, SummaryDB achieves much lower disk usage
and lower time-based range query latencies compared to other TSDBs. These
benefits come at the cost of higher error bounds of the query results.

SummaryDB is best suited for high volumes of numerical data, and it currently
allows for querying of the following metrics across time:
1. Max
2. Min
3. Count
4. Sum

On generic data, it supports:
1. Membership (using bloom filters)

---

### Dependencies

1. [BadgerDB](https://github.com/dgraph-io/badger) is the persistent key-value
store.
2. [Ristretto](https://github.com/dgraph-io/ristretto) is the cache for the
backing disk storage.
3. [Cap'n Proto](https://capnproto.org/) is the serialization format for
on-disk representation for all the window data.
