# nifi-cql-bundle

CQL processors and controller services for Apache NiFi, for Apache Cassandra and ScyllaDB:

- `ExecuteCQL` runs a select, insert, update or delete, with `cql.arg.N` parameters.
- `ExecuteCQLQueryRecord` and `PutCQLRecord` read and write record sets.
- `CQLDistributedMapCache` backs NiFi's distributed map cache with a CQL table.
- Separate Cassandra and ScyllaDB connection services, each carrying only its own driver.

This project is independent of the Apache NiFi project and the Apache Software Foundation, and is
not endorsed by them. Parts of it are derived from the Cassandra bundle of Apache NiFi 1.x, and,
like that code, it is licensed under the Apache License 2.0 (see `LICENSE` and `NOTICE`).

Maven group: `io.github.mikethomsen`. Java packages: `io.github.mikethomsen.nifi.cql`.

## Install

Copy the NARs you need into NiFi's `lib` directory, or its extensions directory, and restart NiFi.
They are published to Maven Central as `io.github.mikethomsen:<artifactId>:1.0.0`, type `nar`.

| NAR | Needed for |
|---|---|
| `nifi-cql-services-api-nar` | Always. The other NARs depend on it. |
| `nifi-cassandra-session-provider-service-nar` | Connecting to Apache Cassandra |
| `nifi-scylladb-session-provider-service-nar` | Connecting to ScyllaDB |
| `nifi-cql-nar` | The processors: `ExecuteCQL`, `ExecuteCQLQueryRecord` and `PutCQLRecord` |
| `nifi-cql-distributedmapcache-service-nar` | `CQLDistributedMapCache` |

Install at least one of the two connection NARs; each carries only its own driver, so both can be
installed together. Built and tested against NiFi 2.12.0.

## Build

The build requires JDK 21, the version NiFi 2.x runs on, and stops at once on any other.

```bash
mvn clean install                                                    # unit tests
mvn -Pintegration-tests verify                                       # adds the Cassandra and ScyllaDB integration tests (needs Docker)
mvn -Pintegration-tests -DTEST_CQL_CONNECTION_TESTS=true verify      # also the authentication and TLS tests, as CI runs
```

Without `-DTEST_CQL_CONNECTION_TESTS=true`, the authentication and TLS tests are reported as skipped.

ScyllaDB needs about 1.05 million asynchronous I/O contexts per instance, counted against the
kernel's system-wide `fs.aio-max-nr`. Any other ScyllaDB on the machine, such as a local container,
uses the same pool. When too few are left, each ScyllaDB test container times out waiting for
"initialization completed". Compare `/proc/sys/fs/aio-nr` with `/proc/sys/fs/aio-max-nr`, and raise
the limit if needed, for example `sudo sysctl -w fs.aio-max-nr=3145728`. Add the same setting to a
file in `/etc/sysctl.d/` to keep it after a reboot.

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

## Release notes

### 1.0.0

The first release as an independent project. Compared with the in-tree NiFi bundle (NIFI-9770),
besides the renames above:

New

- `ExecuteCQL` runs a select, insert, update or delete with `cql.arg.N` parameters, writing selects
  as newline-delimited JSON.
- `CQLDistributedMapCache`, a distributed map cache client backed by a CQL table.

Security

- TLS hostname validation is on by default (Hostname Validation).
- Username and Password must be set together.
- With value logging off, conversion errors name the field, the column type and the forms it
  accepts, but not the value. See [Logging bound values](#logging-bound-values).

Writing (`PutCQLRecord`, and `ExecuteCQL` parameters)

- A number that does not fit its column, or has a fraction the column cannot hold, is rejected
  instead of being wrapped or truncated. Whole-number text such as `42.0` is accepted for integer
  columns. Decimal and varint text is limited to 10,000 characters, and a varint to 10,000 digits.
- Null fields are left unset by default (Null Value Handling).
- Record fields convert the same way as `ExecuteCQL` parameters: bytes for a blob, ISO-8601 text or
  epoch milliseconds for a timestamp, numbers or numeric text for a decimal or varint, address
  literals for an inet (host names are rejected, not resolved), and any collection for a list or
  set. Counter columns accept numeric text, and duration and tuple columns can be written.
- A Time To Live must be at least a second and at most 20 years. A Default Time To Live from
  Expression Language is checked when the service is enabled.
- The Timestamp Field is written as a column only when the table has a column of that name. A field
  that only feeds a primary key override is not written as a column when the table lacks it.

Reading (`ExecuteCQLQueryRecord` and `ExecuteCQL` selects)

- `ExecuteCQLQueryRecord` runs only a SELECT.
- A record writer failure no longer rolls the session back and returns the incoming FlowFile to its
  queue; the writer's error is logged.
- Set values become lists, blobs byte arrays, and map keys text, so the JSON and Avro record writers
  accept them.
- Results carry the incoming FlowFile's attributes, apart from its uuid, fragment and schema
  attributes, and provenance records them as forked from it. Retries keep the incoming attributes.
- Unaliased function columns and repeated columns get legal, distinct field names.
- UDT values are records and tuples are CQL text. inet values have no leading slash.

Map cache

- Every failure is reported as an `IOException`.
