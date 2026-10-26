/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.mikethomsen.nifi.cql.it;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import io.github.mikethomsen.nifi.cql.api.constants.CqlBatchType;
import io.github.mikethomsen.nifi.cql.api.constants.CqlStatementType;
import io.github.mikethomsen.nifi.cql.api.constants.UpdateMethod;
import io.github.mikethomsen.nifi.cql.api.exception.QueryFailureException;
import io.github.mikethomsen.nifi.cql.api.lookup.CqlRow;
import io.github.mikethomsen.nifi.cql.api.lookup.CqlStatementResult;
import io.github.mikethomsen.nifi.cql.api.metadata.QualifiedTableName;
import io.github.mikethomsen.nifi.cql.api.service.AbstractCQLExecutionService;
import io.github.mikethomsen.nifi.cql.api.service.CQLExecutionService;
import io.github.mikethomsen.nifi.cql.api.service.QueryOverrides;
import io.github.mikethomsen.nifi.cql.api.service.WriteOverrides;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises the CRUD-oriented public methods of {@link CQLExecutionService} - {@code insert}, {@code update},
 * {@code query}, and {@code execute} - against whichever backend the concrete subclass wires up. Assumes
 * the subclass has already bootstrapped a {@code testspace} keyspace containing the {@code message},
 * {@code counter_test}, {@code simple_set_test}, and {@code query_test} tables (see {@code init.cql} in the
 * Cassandra module for the canonical schema) and has called {@link #initializeSessionProvider(CqlConnectionInfo)}.
 * <p>
 * Every test here turns on a server- or driver-observable effect a unit test can only assume: a counter's
 * running total, a conditional write handing back the row that beat it, an explicit {@code WRITETIME} or
 * {@code TTL} taking hold, a LOGGED batch refusing a counter mutation, an unsatisfiable consistency level
 * failing on the first {@code execute()}. The driver-metadata-to-{@code PrimaryKey} mapping that backs
 * {@code getMetadata} is covered without a cluster in {@code CassandraTableMetadataMappingTest}.
 * <p>
 * Setup is a plain protected method rather than a JUnit lifecycle callback (e.g. {@code @BeforeAll})
 * deliberately: on a {@code @ParameterizedClass} leaf, {@code @BeforeAll} - even inherited from this
 * superclass - runs BEFORE the leaf's own {@code @BeforeParameterizedClassInvocation} method, so an
 * inherited {@code @BeforeAll} here would run before the container (and connection info) it depends on
 * exists. Calling this method explicitly from the leaf's {@code @BeforeParameterizedClassInvocation} avoids
 * that ordering trap entirely.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractCqlCrudIT {

    protected CQLExecutionService sessionProvider;

    private CqlSession session;

    private CqlConnectionInfo connectionInfo;

    private String kvTableName;

    /**
     * @return a fresh, unconfigured instance of the backend implementation under test
     */
    protected abstract CQLExecutionService newSessionProvider();

    protected void initializeSessionProvider(final CqlConnectionInfo connectionInfo) throws Exception {
        this.connectionInfo = connectionInfo;
        this.session = connectionInfo.session();

        this.kvTableName = connectionInfo.keyspace() + ".execute_kv";
        CqlDdl.executeWithRetry(session, "create table if not exists " + kvTableName + " (k blob primary key, v blob)");

        this.sessionProvider = CqlServiceRunner.forService(newSessionProvider())
                .withConnection(connectionInfo)
                .enable();
    }

    private static MapRecord messageRecord(final String sender, final String receiver, final String message, final Instant whenSent) {
        final RecordSchema schema = new SimpleRecordSchema(List.of(
                new RecordField("sender", RecordFieldType.STRING.getDataType()),
                new RecordField("receiver", RecordFieldType.STRING.getDataType()),
                new RecordField("message", RecordFieldType.STRING.getDataType()),
                new RecordField("when_sent", RecordFieldType.TIMESTAMP.getDataType())));
        // A HashMap rather than Map.of, which rejects the null message some tests need.
        final Map<String, Object> values = new HashMap<>();
        values.put("sender", sender);
        values.put("receiver", receiver);
        values.put("message", message);
        values.put("when_sent", whenSent);
        return new MapRecord(schema, values);
    }

    private static MapRecord counterRecord(final String key) {
        final RecordSchema schema = new SimpleRecordSchema(List.of(
                new RecordField("column_a", RecordFieldType.STRING.getDataType()),
                new RecordField("increment_field", RecordFieldType.INT.getDataType())));
        return new MapRecord(schema, Map.of("column_a", key, "increment_field", 1));
    }

    private long counterValue(final String key) {
        return session.execute("select increment_field from testspace.counter_test where column_a = '" + key + "'")
                .one().getLong("increment_field");
    }

    @Test
    @DisplayName("A null field leaves its column untouched when nulls are unset, and clears it when they are written")
    void testNullFieldIsUnsetOrWritten() {
        final Instant whenSent = Instant.parse("2026-01-02T03:04:05Z");
        final QualifiedTableName table = new QualifiedTableName(null, "message");

        sessionProvider.insert(table, messageRecord("null.handling", "jane.smith", "hello", whenSent), Map.of(), WriteOverrides.NONE);

        final MapRecord sparse = messageRecord("null.handling", "jane.smith", null, whenSent);
        final String select = "select message from testspace.message where sender = 'null.handling'";

        sessionProvider.insert(table, sparse, Map.of(), new WriteOverrides(null, null, true));
        assertEquals("hello", session.execute(select).one().getString("message"));

        sessionProvider.insert(table, List.of(sparse), Map.of(), CqlBatchType.UNLOGGED, new WriteOverrides(null, null, true));
        assertEquals("hello", session.execute(select).one().getString("message"));

        sessionProvider.insert(table, sparse, Map.of(), new WriteOverrides(null, null, false));
        assertTrue(session.execute(select).one().isNull("message"));
    }

    @Test
    @DisplayName("Incrementing then decrementing a counter column produces the expected running total")
    void testIncrementAndDecrement() {
        final QualifiedTableName table = new QualifiedTableName(null, "counter_test");
        final MapRecord record = counterRecord("abcdef");
        final List<String> updateKeys = List.of("column_a");

        sessionProvider.update(table, record, Map.of(), updateKeys, UpdateMethod.INCREMENT, WriteOverrides.NONE);
        sessionProvider.update(table, record, Map.of(), updateKeys, UpdateMethod.INCREMENT, WriteOverrides.NONE);
        assertEquals(2, counterValue("abcdef"));

        sessionProvider.update(table, record, Map.of(), updateKeys, UpdateMethod.DECREMENT, WriteOverrides.NONE);
        assertEquals(1, counterValue("abcdef"));
    }

    @Test
    @DisplayName("Batch-updating counters requires a COUNTER batch type - Cassandra/ScyllaDB reject counter mutations in a LOGGED batch")
    void testBatchCounterUpdateRequiresCounterBatchType() {
        final List<String> keys = List.of("batch-counter-a", "batch-counter-b", "batch-counter-c");
        final List<org.apache.nifi.serialization.record.Record> records = new ArrayList<>();
        keys.forEach(key -> records.add(counterRecord(key)));

        List<String> updateKeys = List.of("column_a");

        // A LOGGED (or UNLOGGED) batch cannot contain counter mutations - the server rejects it outright.
        // This is exactly the bug PutCQLRecord had: BATCH_STATEMENT_TYPE was validated but never forwarded,
        // so every batched counter increment/decrement was silently submitted as LOGGED and would have
        // failed here before the fix.
        assertThrows(Exception.class, () -> sessionProvider.update(new QualifiedTableName(null, "counter_test"), records, Map.of(), updateKeys,
                UpdateMethod.INCREMENT, CqlBatchType.LOGGED, WriteOverrides.NONE));

        sessionProvider.update(new QualifiedTableName(null, "counter_test"), records, Map.of(), updateKeys, UpdateMethod.INCREMENT, CqlBatchType.COUNTER, WriteOverrides.NONE);

        for (final String key : keys) {
            assertEquals(1, counterValue(key));
        }
    }

    @Test
    @DisplayName("Inserting a record with a TTL override sets an expiration on the written columns")
    void testInsertWithTtlOverrideSetsExpiration() {
        MapRecord record = messageRecord("ttl.sender", "ttl.receiver", "expires soon", Instant.now());

        sessionProvider.insert(new QualifiedTableName(null, "message"), record, Map.of(), new WriteOverrides(Duration.ofSeconds(100), null, false));

        // "when_sent" (the last clustering column) is deliberately omitted: sender + receiver alone
        // uniquely identify the single row this test wrote, and CQL allows a prefix of clustering
        // columns in an equality filter without needing ALLOW FILTERING or reconstructing the exact
        // timestamp literal that was generated for "when_sent".
        Row row = session.execute(
                "select TTL(message) as remaining_ttl from testspace.message where sender = 'ttl.sender' and receiver = 'ttl.receiver'")
                .iterator().next();

        int remainingTtl = row.getInt("remaining_ttl");
        assertTrue(remainingTtl > 0 && remainingTtl <= 100, "Expected a remaining TTL between 1 and 100 seconds but was " + remainingTtl);
    }

    @Test
    @DisplayName("Inserting with a Timestamp Field override uses that record field's value as the CQL write timestamp")
    void testInsertWithTimestampFieldOverrideUsesRecordValue() {
        Instant eventTime = Instant.now().minus(Duration.ofDays(30)).truncatedTo(ChronoUnit.MILLIS);
        MapRecord record = messageRecord("timestamp.sender", "timestamp.receiver", "old event", eventTime);

        sessionProvider.insert(new QualifiedTableName(null, "message"), record, Map.of(), new WriteOverrides(null, "when_sent", false));

        long writeTimeMicros = session.execute(
                "select WRITETIME(message) as write_time from testspace.message where sender = 'timestamp.sender' and receiver = 'timestamp.receiver'")
                .iterator().next().getLong("write_time");

        assertEquals(toEpochMicros(eventTime), writeTimeMicros);
    }

    @Test
    @DisplayName("Each record in a batch insert carries its own write timestamp when a Timestamp Field override is set")
    void testBatchInsertWithTimestampFieldOverrideUsesPerRecordValues() {
        Instant olderEventTime = Instant.now().minus(Duration.ofDays(30)).truncatedTo(ChronoUnit.MILLIS);
        Instant newerEventTime = Instant.now().minus(Duration.ofDays(1)).truncatedTo(ChronoUnit.MILLIS);

        List<org.apache.nifi.serialization.record.Record> records = List.of(
                messageRecord("batch.timestamp.sender", "older.receiver", "older event", olderEventTime),
                messageRecord("batch.timestamp.sender", "newer.receiver", "newer event", newerEventTime));

        sessionProvider.insert(new QualifiedTableName(null, "message"), records, Map.of(), CqlBatchType.LOGGED, new WriteOverrides(null, "when_sent", false));

        assertEquals(toEpochMicros(olderEventTime), writeTimeForBatchTimestampSender("older.receiver"));
        assertEquals(toEpochMicros(newerEventTime), writeTimeForBatchTimestampSender("newer.receiver"));
    }

    @Test
    @DisplayName("Updating with a Timestamp Field override uses that record field's value as the CQL write timestamp")
    void testUpdateSetWithTimestampFieldOverrideUsesRecordValue() {
        // "when_sent" doubles as both an update key (part of message's primary key, so it identifies which row
        // to update) and the Timestamp Field source - a realistic pattern, since it's a real column already
        // being written, not an extra field invented just to carry a timestamp. generateUpdate() only knows
        // how to treat a record field as either a SET target or a WHERE key, so the Timestamp Field has to be
        // one of those, not an arbitrary extra field.
        //
        // The row-establishing insert below deliberately writes with an explicit, much older timestamp than
        // "when_sent" itself (which only serves as the row's identity here). Without that, this insert's own
        // auto-assigned "now" timestamp would land only a fraction of a millisecond away from the override
        // under test below, and Cassandra's last-write-wins would resolve that near-tie essentially at random -
        // making the test flaky/misleading regardless of whether the override actually works.
        Instant whenSent = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        long deliberatelyOldWriteTimestamp = toEpochMicros(whenSent.minus(Duration.ofDays(60)));
        session.execute(String.format(
                "insert into testspace.message (sender, receiver, message, when_sent) values "
                        + "('update.timestamp.sender', 'update.timestamp.receiver', 'original', %d) using timestamp %d",
                whenSent.toEpochMilli(), deliberatelyOldWriteTimestamp));

        MapRecord record = messageRecord("update.timestamp.sender", "update.timestamp.receiver", "updated", whenSent);
        List<String> updateKeys = List.of("sender", "receiver", "when_sent");

        sessionProvider.update(new QualifiedTableName(null, "message"), record, Map.of(), updateKeys, UpdateMethod.SET, new WriteOverrides(null, "when_sent", false));

        long writeTimeMicros = session.execute(String.format(
                "select WRITETIME(message) as write_time from testspace.message where sender = 'update.timestamp.sender' "
                        + "and receiver = 'update.timestamp.receiver' and when_sent = %d", whenSent.toEpochMilli()))
                .iterator().next().getLong("write_time");

        assertEquals(toEpochMicros(whenSent), writeTimeMicros);
    }

    private long writeTimeForBatchTimestampSender(String receiver) {
        return session.execute(String.format(
                "select WRITETIME(message) as write_time from testspace.message where sender = 'batch.timestamp.sender' and receiver = '%s'", receiver))
                .iterator().next().getLong("write_time");
    }

    private static long toEpochMicros(Instant instant) {
        return instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1_000L;
    }

    @Test
    @DisplayName("TTL and Timestamp Field overrides are ignored for counter updates, since Cassandra/ScyllaDB support neither on counter columns")
    void testCounterUpdateIgnoresWriteOverrides() {
        final QualifiedTableName table = new QualifiedTableName(null, "counter_test");
        final List<String> updateKeys = List.of("column_a");

        // If either override were mistakenly applied to a counter UPDATE, Cassandra/ScyllaDB would reject the
        // statement outright ("Cannot set ttl on a counter column", or a refusal of the custom timestamp), so
        // not throwing proves it was skipped. "column_a" is not a timestamp field, which does not matter: the
        // override must be dropped before the field is ever read.
        assertDoesNotThrow(() -> sessionProvider.update(table, counterRecord("ttl-ignored-for-counters"), Map.of(), updateKeys,
                UpdateMethod.INCREMENT, new WriteOverrides(Duration.ofSeconds(100), null, false)), "TTL override");
        assertDoesNotThrow(() -> sessionProvider.update(table, counterRecord("timestamp-ignored-for-counters"), Map.of(), updateKeys,
                UpdateMethod.INCREMENT, new WriteOverrides(null, "column_a", false)), "Timestamp Field override");
    }

    @Test
    @DisplayName("Updating a record with the SET method overwrites the existing column value")
    void testUpdateSet() throws Exception {
        session.execute("insert into testspace.simple_set_test(username, is_active) values('john.smith', true)");
        Thread.sleep(250);

        RecordSchema schema = new SimpleRecordSchema(List.of(
                new RecordField("username", RecordFieldType.STRING.getDataType()),
                new RecordField("is_active", RecordFieldType.BOOLEAN.getDataType())));
        MapRecord record = new MapRecord(schema, Map.of("username", "john.smith", "is_active", false));

        sessionProvider.update(new QualifiedTableName(null, "simple_set_test"), record, Map.of(), List.of("username"), UpdateMethod.SET, WriteOverrides.NONE);

        Row row = session.execute("select is_active from testspace.simple_set_test where username = 'john.smith'").one();
        assertFalse(row.getBoolean("is_active"));
    }

    @Test
    @DisplayName("Querying rows via the CQL execution service returns them without error")
    void testQueryRecord() {
        String[] statements = """
                insert into testspace.query_test (column_a, column_b, when)
                    values ('abc', 'def', toTimestamp(now()));
                insert into testspace.query_test (column_a, column_b, when)
                    values ('abc', 'ghi', toTimestamp(now()));
                insert into testspace.query_test (column_a, column_b, when)
                    values ('abc', 'jkl', toTimestamp(now()));
            """.trim().split("\\;");
        for (String statement : statements) {
            session.execute(statement);
        }

        CollectingCqlQueryCallback callback = new CollectingCqlQueryCallback();

        sessionProvider.query("select * from testspace.query_test", null, callback, QueryOverrides.NONE);

        assertTrue(callback.getRecords().size() >= statements.length,
                () -> "Expected at least the " + statements.length + " inserted rows back, got " + callback.getRecords().size());
    }

    @Test
    @DisplayName("A query that fails on the driver's initial execute() surfaces as QueryFailureException, not a raw driver exception")
    void testQueryExecutionFailureOnInitialExecuteIsWrappedAsQueryFailureException() throws Exception {
        // A consistency level of THREE can never be satisfied by this single-node cluster, so the driver
        // throws UnavailableException (a QueryExecutionException) synchronously from the very first
        // execute() call, before any rows are fetched - exactly the code path that used to bypass the
        // QueryFailureException translation.
        final CQLExecutionService unsatisfiableConsistencyProvider = CqlServiceRunner.forService(newSessionProvider())
                .withConnection(connectionInfo)
                .withProperty(AbstractCQLExecutionService.CONSISTENCY_LEVEL, "THREE")
                .enable();

        final CollectingCqlQueryCallback callback = new CollectingCqlQueryCallback();

        assertThrows(QueryFailureException.class, () -> unsatisfiableConsistencyProvider.query(
                "select * from testspace.query_test", null, callback, QueryOverrides.NONE));

        assertTrue(callback.getRecords().isEmpty(), "A query that never executed should not have delivered rows");
    }

    // ---- execute(): conditional writes and schema-free reads -------------------------------------------
    //
    // These are the paths query() cannot express: a statement-level outcome, and values read without a
    // schema imposed on them. Both matter for a caller doing compare-and-set over opaque bytes.

    /**
     * The {@code keyspace.table} name the {@code execute()} tests below share. The table itself is created
     * once in {@link #initializeSessionProvider(CqlConnectionInfo)}, before the service session exists, so
     * that session sees it on connect rather than racing a lazy create against its schema metadata.
     */
    private String kvTable() {
        return kvTableName;
    }

    private static ByteBuffer bytes(final String value) {
        return ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("An unconditional statement reports wasApplied, so a caller that never writes conditionally has no special case")
    void testExecuteUnconditionalWriteIsApplied() {
        final String table = kvTable();

        final CqlStatementResult result = sessionProvider.execute("insert into " + table + " (k, v) values (?, ?)",
                List.of(bytes("uncond"), bytes("value")), QueryOverrides.NONE);

        assertTrue(result.wasApplied());
        assertTrue(result.rows().isEmpty(), "an unconditional insert returns no rows");
    }

    @Test
    @DisplayName("A conditional write applies once with no row, and is then rejected with the stored row, which is what makes compare-and-set one round trip")
    void testExecuteConditionalWrite() {
        final String table = kvTable();
        // A NUL byte in the stored value shows blobs come back as the exact bytes, with no encoding imposed.
        final byte[] original = "opaque\0bytes".getBytes(StandardCharsets.UTF_8);

        final CqlStatementResult applied = sessionProvider.execute(
                "insert into " + table + " (k, v) values (?, ?) if not exists",
                List.of(bytes("lwt-existing"), ByteBuffer.wrap(original)), QueryOverrides.NONE);
        assertTrue(applied.wasApplied());
        assertTrue(applied.rows().isEmpty(),
                () -> "expected no rows for an applied conditional write, got " + applied.rows().size());

        final CqlStatementResult result = sessionProvider.execute(
                "insert into " + table + " (k, v) values (?, ?) if not exists",
                List.of(bytes("lwt-existing"), bytes("attempted")), QueryOverrides.NONE);

        assertFalse(result.wasApplied());
        assertEquals(1, result.rows().size());

        final CqlRow stored = result.rows().getFirst();
        assertArrayEquals(original, stored.getBytes("v"),
                "the losing writer must see the value that is actually stored");

        // The outcome is reported by wasApplied(), not as data: "[applied]" is not a legal identifier, and
        // surfacing it as a column would push that problem onto every caller.
        assertFalse(stored.columnNames().contains("[applied]"),
                () -> "the outcome column leaked into the row: " + stored.columnNames());
    }

    @Test
    @DisplayName("A conditional delete reports whether it applied and returns the row it removed")
    void testExecuteConditionalDelete() {
        final String table = kvTable();
        sessionProvider.execute("insert into " + table + " (k, v) values (?, ?)",
                List.of(bytes("to-delete"), bytes("doomed")), QueryOverrides.NONE);

        final CqlStatementResult deleted = sessionProvider.execute(
                "delete from " + table + " where k = ? if exists", List.of(bytes("to-delete")), QueryOverrides.NONE);
        assertTrue(deleted.wasApplied());

        final CqlStatementResult again = sessionProvider.execute(
                "delete from " + table + " where k = ? if exists", List.of(bytes("to-delete")), QueryOverrides.NONE);
        assertFalse(again.wasApplied(), "deleting an absent row must not report as applied");
    }

    @Test
    @DisplayName("Cells arrive in selection order, and a column name that is not a legal identifier survives intact")
    void testExecuteExposesCellsInSelectionOrder() {
        final String table = kvTable();
        sessionProvider.execute("insert into " + table + " (k, v) values (?, ?)",
                List.of(bytes("cells"), bytes("value")), QueryOverrides.NONE);

        final CqlStatementResult result = sessionProvider.execute(
                "select v, writetime(v), k from " + table + " where k = ?", List.of(bytes("cells")), QueryOverrides.NONE);

        final CqlRow row = result.rows().getFirst();
        assertEquals(List.of("v", "writetime(v)", "k"), row.columnNames(),
                "selection order is the only ordering information a schema-free caller has");
        assertNotNull(row.getObject("writetime(v)"),
                "a projected function's column name is not a legal identifier, and must survive anyway");
    }

    @Test
    @DisplayName("A query returning no rows yields an empty result rather than a null one")
    void testExecuteEmptyResult() {
        final String table = kvTable();

        final CqlStatementResult result = sessionProvider.execute("select v from " + table + " where k = ?",
                List.of(bytes("never-written")), QueryOverrides.NONE);

        assertTrue(result.rows().isEmpty());
        assertTrue(result.wasApplied(), "a plain SELECT is unconditional");
    }

    @Test
    @DisplayName("validateStatement accepts a sound statement and rejects one the cluster cannot prepare or whose markers do not match the parameters")
    void testValidateStatement() {
        assertDoesNotThrow(() -> sessionProvider.validateStatement("select * from message where sender = ?", CqlStatementType.SELECT, 1));
        assertDoesNotThrow(() -> sessionProvider.validateStatement(
                "update message set message = ? where sender = ? and receiver = ? and when_sent = ?", CqlStatementType.UPDATE, 4));
        assertDoesNotThrow(() -> sessionProvider.validateStatement(
                "delete from message where sender = ? and receiver = ? and when_sent = ?", CqlStatementType.DELETE, 3));

        // The wrong kind of statement and a missing WHERE are rejected before preparing, and covered in
        // CassandraCQLExecutionServicePrepareFailureTest. These need the cluster's own view of the statement.
        // Marker count does not match the parameters.
        assertThrows(IllegalArgumentException.class, () -> sessionProvider.validateStatement(
                "select * from message where sender = ?", CqlStatementType.SELECT, 2));
        // The cluster itself rejects these: a table that does not exist, a column that does not exist, and a syntax error.
        assertThrows(IllegalArgumentException.class, () -> sessionProvider.validateStatement(
                "select * from no_such_table where sender = ?", CqlStatementType.SELECT, 1));
        assertThrows(IllegalArgumentException.class, () -> sessionProvider.validateStatement(
                "select * from message where no_such_column = ?", CqlStatementType.SELECT, 1));
        assertThrows(IllegalArgumentException.class, () -> sessionProvider.validateStatement(
                "select * frm message where sender = ?", CqlStatementType.SELECT, 1));
    }
}
