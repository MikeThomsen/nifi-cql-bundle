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

The same property controls whether a value that cannot be converted for its column appears in the
error. With it off, the error names the field or parameter and the column type, and says what was
expected, but leaves out the value and any underlying error that would repeat it. Since those errors
reach `nifi-app.log` and the processor's bulletins, turning the property on also puts rejected values
there.

## Driver Configuration File

The connection services accept an optional Java driver configuration file (HOCON, such as an
`application.conf`) for driver settings that have no property of their own. Settings in the file take
precedence over the service's properties, and nothing in the NiFi UI shows that a property has been
overridden. What the file can and cannot change:

| Setting | When set on the service | When not set on the service |
|---|---|---|
| SSL Context Service and Hostname Validation | The service's TLS is used; the file cannot change it | The file can turn TLS on with its own engine factory and settings |
| Username and Password | The service's credentials are used | The file can configure any authentication provider |
| Cassandra Datacenter | The service's value is used | Not applicable: the property is required |
| Cassandra Contact Points | The file's contact points are added to these | Not applicable: the property is required |
| Consistency Level, timeouts, compression | **The file's value wins** | The file's value, or the driver default |
| Anything else: load balancing, retries, request trackers and other plug-ins | Not a service property | The file's value, or the driver default |

The file can also name any driver plug-in class on NiFi's classpath, include other files and read
environment variables. Treat it like code:

- Keep it readable by the NiFi user and writable only by administrators.
- Give edit rights on the connection services only to people you would let change how NiFi connects
  to the cluster, since anyone who can set the property can point it at a file of their own.
- Keep secrets out of it; use the service's Username and Password, which NiFi stores encrypted.

## Migrating from the in-tree NiFi bundle

The class names and bundle coordinates changed, so flows exported with the old names need their
processors and services re-pointed:

| Old | New |
|---|---|
| `org.apache.nifi.processors.cql.*` | `io.github.mikethomsen.nifi.cql.processors.*` |
| `org.apache.nifi.service.cql.api.*` | `io.github.mikethomsen.nifi.cql.api.*` |
| `org.apache.nifi:nifi-cql-*` | `io.github.mikethomsen:nifi-cql-*` |
