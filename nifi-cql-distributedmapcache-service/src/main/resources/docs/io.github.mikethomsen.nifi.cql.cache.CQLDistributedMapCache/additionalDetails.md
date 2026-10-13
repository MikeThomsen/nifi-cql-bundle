<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# CQLDistributedMapCache

## Description

_CQLDistributedMapCache_ provides a `DistributedMapCacheClient` backed by Apache Cassandra or ScyllaDB,
storing each cache entry as a single row in a table you create and manage. Any component that accepts a
_Distributed Map Cache Client_ - `DetectDuplicate`, `DeduplicateRecord`, `PutDistributedMapCache`,
`FetchDistributedMapCache` and others - can use it.

It opens no connection of its own. All cluster access goes through the _CQL Execution Service_ you configure
(`CassandraCQLExecutionService` or `ScyllaDBCQLExecutionService`), so contact points, credentials, TLS,
consistency level and timeouts are configured once on that service and shared. This service adds only the
table it stores entries in.

## Table design

The table is **not created for you**. It must exist before the service is enabled, and its shape is fixed by
what a map cache is: one row per key, keyed by the key itself.

### Required schema

| Column | CQL type | Role |
|---|---|---|
| _Key Column Name_ (default `cache_key`) | `blob` | Partition key. One partition per cache entry |
| _Value Column Name_ (default `cache_value`) | `blob` | The stored value |

```sql
CREATE TABLE my_keyspace.cache (
    cache_key   blob PRIMARY KEY,
    cache_value blob
);
```

The table may contain other columns; this service never reads or writes them, and never issues DDL of any
kind - no `CREATE`, no `ALTER`, no `TRUNCATE`.

### Why both columns must be `blob`

This is a requirement, not a default. A `DistributedMapCacheClient` receives a `Serializer` for the key and
the value, and those have already reduced the caller's Java objects to opaque bytes before this service sees
them. It has no schema for those bytes and no contract that would let it interpret them.

`blob` is therefore the only column type that can hold them safely. A `text` column, for example, would force
the bytes through a character encoding that the caller never agreed to, and any value that is not valid UTF-8
would be corrupted or rejected. If you point this service at a `text` key column, writes fail at the server
rather than silently mangling data - but the failure is avoidable by using the right type up front.

### Partition layout

Each entry is its own partition. That is the correct and safest shape for a key/value access pattern: every
read and write is a single-partition operation, there is no partition that grows over time, and no
coordinator has to fan out across replicas to answer a lookup.

The trade-off is per-partition overhead. Cassandra and ScyllaDB carry bookkeeping per partition - bloom
filters, index summaries, key cache entries - so a table with billions of entries is a table with billions of
partitions, and that overhead becomes the binding constraint before raw data volume does. For caches at that
scale, plan capacity around partition count rather than bytes stored.

### Cluster-side maintenance

Nothing here compacts, repairs or expires on your behalf beyond the TTL described below. In particular,
entries removed with `remove()` leave tombstones, which are reclaimed on the table's own
`gc_grace_seconds` schedule. A cache with heavy churn benefits from a TTL - see below - and from a
compaction strategy chosen for the workload rather than the default.

## Configuration example

Given hash-to-metadata lookups in a keyspace called `lookups`:

```sql
CREATE KEYSPACE IF NOT EXISTS lookups
    WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 3};

CREATE TABLE lookups.file_hashes (
    hash     blob PRIMARY KEY,
    metadata blob
);
```

Configure a session provider, then this service:

| Property | Value |
|---|---|
| CQL Execution Service | `CassandraCQLExecutionService` (already configured for the cluster) |
| Table Name | `lookups.file_hashes` |
| Key Column Name | `hash` |
| Value Column Name | `metadata` |
| Time To Live | `30 days` |
| Strict Removal | `false` |
| Maximum Value Size | `1 MB` |

A `DetectDuplicate` processor pointed at this service now records each hash it has seen, and routes a
FlowFile whose hash is already present to `duplicate`. The entry expires 30 days after it is written, so the
deduplication window is 30 days rather than forever.

_Table Name_ may be given as `<keyspace>.<table>` or as `<table>` alone, in which case the keyspace
configured on the CQL Execution Service is used.

## Operations

### Reads and unconditional writes

`put` issues a single `INSERT`. Because a CQL write is an upsert, putting a key that already exists replaces
its value, exactly as the cache contract requires.

`get` selects the value column for one key. `containsKey` selects the **key** column with `LIMIT 1` rather
than the value, so an existence check never pays to transfer a value it is going to discard.

### Conditional writes

`putIfAbsent` and `getAndPutIfAbsent` are implemented with a **lightweight transaction** (`IF NOT EXISTS`).
The cluster resolves these through a Paxos consensus round, so among concurrent callers exactly one is told
it created the entry. This is what makes the service usable for deduplication: implementing the same
operations as "read, then write if absent" would let two callers both observe the key as absent and both
believe they created it.

`getAndPutIfAbsent` costs no more than `putIfAbsent`. When the conditional write is rejected, the server
returns the row that beat it alongside the outcome, so the existing value arrives in the same round trip.

Lightweight transactions are **markedly more expensive** than ordinary writes - a consensus round rather than
a single write - so use `put` where you do not need the guarantee.

### Removal

`remove` behaves differently depending on _Strict Removal_:

| _Strict Removal_ | Statements | Return value |
|---|---|---|
| `false` (default) | An existence check, then an unconditional `DELETE` | Best-effort. A concurrent writer can make it stale |
| `true` | A single conditional `DELETE ... IF EXISTS` | Correct under concurrency, at the cost of a lightweight transaction |

Leave it `false` unless a caller acts on the return value. Most do not - they remove an entry and ignore
whether it was there.

`removeAndGet` **is not atomic, and cannot be made so.** A conditional delete reports whether it applied but
does not return the row it removed, and CQL has no delete-and-return statement, so the value is read first
and then deleted. A writer that changes the value between those two statements will see the older of the two
returned. The delete itself is always conditional, so a `null` return means the entry was already gone rather
than that it held nothing.

### Bulk operations

`subMap` issues one statement per requested key rather than a single `IN` clause. Keys are partition keys, so
an `IN` across them makes one coordinator responsible for fanning out to every replica involved and holding
the entire result - the pattern Cassandra practice warns against. Sequential lookups are more round trips but
bounded work per node.

`keySet` is **not supported** and throws `UnsupportedOperationException`. Enumerating every key is a full
table scan - every partition on every replica, with the whole key set materialized in NiFi's heap - which is
too expensive an operation on a CQL data store to offer behind a cache interface. Components that depend on
`keySet` are not compatible with this cache.

## Time To Live

_Time To Live_ appends `USING TTL` to every write this service performs, so entries expire without any
explicit removal. It applies to both unconditional and conditional writes, so an entry created by
`putIfAbsent` expires on the same terms as one created by `put`.

Leave it unset for entries that never expire. Changing it affects only entries written afterwards - rows
already stored keep the expiry they were written with, and this service never rewrites them.

For a cache with heavy churn, a TTL is usually preferable to explicit removal: expiry is spread uniformly
across the table, whereas deletes tend to cluster and leave tombstones the next read has to walk.

## Maximum Value Size

Values larger than _Maximum Value Size_ are rejected with a `SerializationException` **before anything is
sent**, rather than being refused by the server or, worse, accepted. Cassandra and ScyllaDB tolerate large
blobs poorly well below their hard limits: large values inflate heap pressure during compaction and repair
and slow every read of the partition holding them.

The default of 1 MB is a conservative ceiling for a cache. Raise it deliberately, having considered what a
value of that size does to the cluster when several million of them exist.
