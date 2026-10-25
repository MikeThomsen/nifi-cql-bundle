# nifi-cql-bundle

CQL processors and controller services for Apache NiFi, for Apache Cassandra and ScyllaDB:

- `ExecuteCQL` runs a select, insert, update or delete, with `cql.arg.N` parameters.
- `ExecuteCQLQueryRecord` and `PutCQLRecord` read and write record sets.
- `CQLDistributedMapCache` backs NiFi's distributed map cache with a CQL table.
- Separate Cassandra and ScyllaDB connection services, each carrying only its own driver.

This project is independent of the Apache NiFi project and the Apache Software Foundation, and is
not endorsed by them. It is derived from the Apache NiFi Cassandra bundle and, like that code, is
licensed under the Apache License 2.0 (see `LICENSE`).

Maven group: `io.github.mikethomsen`. Java packages: `io.github.mikethomsen.nifi.cql`.

## Build

```bash
mvn clean install                          # unit tests
mvn -Pintegration-tests verify             # adds Cassandra and ScyllaDB integration tests (needs Docker)
```

## Logging bound values

At debug level the connection services log each statement they run, but not the values bound to
it. To add the values for diagnosis, start NiFi with the JVM system property
`ENABLE_CQL_VALUE_LOGGING=true` (for example `java.arg.20=-DENABLE_CQL_VALUE_LOGGING=true` in
`bootstrap.conf`) and set the debug level on the logger for the service:

- `io.github.mikethomsen.nifi.cql.cassandra.CassandraCQLExecutionService`
- `io.github.mikethomsen.nifi.cql.scylladb.ScyllaDBCQLExecutionService`

**This writes row data to `nifi-app.log` in clear text**: every query parameter and every field of
every record inserted, updated or deleted, including any personal data or secrets in them. The
property is read once at startup and applies to every service in the JVM. Turn it on only while
diagnosing a problem, on data you are allowed to log, and restart NiFi without it afterwards.

## Migrating from the in-tree NiFi bundle

The class names and bundle coordinates changed, so flows exported with the old names need their
processors and services re-pointed:

| Old | New |
|---|---|
| `org.apache.nifi.processors.cql.*` | `io.github.mikethomsen.nifi.cql.processors.*` |
| `org.apache.nifi.service.cql.api.*` | `io.github.mikethomsen.nifi.cql.api.*` |
| `org.apache.nifi:nifi-cql-*` | `io.github.mikethomsen:nifi-cql-*` |
