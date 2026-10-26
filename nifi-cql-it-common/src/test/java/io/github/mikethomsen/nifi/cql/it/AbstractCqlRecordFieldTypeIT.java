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
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import io.github.mikethomsen.nifi.cql.api.metadata.QualifiedTableName;
import io.github.mikethomsen.nifi.cql.api.service.CQLExecutionService;
import io.github.mikethomsen.nifi.cql.api.service.CQLQueryCallback;
import io.github.mikethomsen.nifi.cql.api.service.QueryOverrides;
import io.github.mikethomsen.nifi.cql.api.service.WriteOverrides;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Date;
import java.sql.Time;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * Exercises {@link CQLExecutionService#insert(String, org.apache.nifi.serialization.record.Record, WriteOverrides)}
 * and {@link CQLExecutionService#query(String, boolean, List, CQLQueryCallback, QueryOverrides)} for the
 * record-field-type to CQL-type pairings that only a real cluster can settle, against whichever backend the
 * concrete subclass wires up. Every case follows the same shape: write a record, read the row back, and assert
 * the value comes back as the type the column holds.
 *
 * <p>What stays here: the UDT cases, whose {@code Record}-to-{@code UdtValue} conversion is driven by the
 * driver's live, server-resolved type metadata (a fabricated {@code UserDefinedType} cannot stand in for
 * that); one round trip each for a list, a set, and a map; the two {@code java.sql.Date}/{@code Time} rows,
 * which bind through a custom codec but read back through the driver's default, so only a real round trip
 * shows the write and the read stay consistent across that asymmetry; and one canonical scalar plus one
 * representative coercion as an end-to-end smoke test of the write/read path.
 *
 * <p>What moved out: the rest of the scalar and coercion grid. Every {@code Flexible*Codec} widening is
 * covered byte-for-byte in {@code FlexibleCodecTest} and {@code CharacterCodecTest}, timestamp parsing in
 * {@code CassandraCQLExecutionServiceBindValueTest}, and {@code convertForCqlType}'s {@code Object[]}
 * handling and the {@code timeuuid} rejection of a non-version-1 UUID in
 * {@code CassandraCQLExecutionServiceWritePathTest}. Running those against a container was a slow,
 * Docker-gated copy of coverage that already exists without one.
 * <p>
 * Setup is a plain protected method rather than a JUnit lifecycle callback for the same reason documented on
 * {@link AbstractCqlCrudIT}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractCqlRecordFieldTypeIT {

    /** The {@code address_item} UDT the collection-of-UDT tests share. */
    private static final RecordSchema ADDRESS_SCHEMA = new SimpleRecordSchema(List.of(
            new RecordField("street_address", RecordFieldType.STRING.getDataType()),
            new RecordField("state", RecordFieldType.STRING.getDataType()),
            new RecordField("zip_code", RecordFieldType.INT.getDataType())));

    protected CQLExecutionService sessionProvider;

    private CqlSession session;

    private String keyspace;

    /**
     * @return a fresh, unconfigured instance of the backend implementation under test
     */
    protected abstract CQLExecutionService newSessionProvider();

    protected void initializeSessionProvider(final CqlConnectionInfo connectionInfo) throws Exception {
        this.session = connectionInfo.session();
        this.keyspace = connectionInfo.keyspace();
        this.sessionProvider = CqlServiceRunner.forService(newSessionProvider())
                .withConnection(connectionInfo)
                .enable();
    }

    /** Idempotent DDL, retried on the failures that say nothing about whether it ran - see {@link CqlDdl}. */
    private void executeDdlWithRetry(final String cql) {
        CqlDdl.executeWithRetry(session, cql);
    }

    private void createTable(final String tableName, final String cqlColumnType) {
        executeDdlWithRetry(String.format("create table if not exists %s.%s (id int primary key, value_field %s)", keyspace, tableName, cqlColumnType));
    }

    private RecordSchema schemaFor(final RecordField valueField) {
        return new SimpleRecordSchema(List.of(new RecordField("id", RecordFieldType.INT.getDataType()), valueField));
    }

    private void insert(final String tableName, final MapRecord record) {
        sessionProvider.insert(new QualifiedTableName(null, tableName), record, Map.of(), WriteOverrides.NONE);
    }

    /** Creates the {@code address_item} UDT that {@link #ADDRESS_SCHEMA} describes. */
    private void createAddressType() {
        executeDdlWithRetry("create type if not exists " + keyspace + ".address_item (street_address text, state text, zip_code int)");
    }

    private static MapRecord address(final String street, final String state, final int zipCode) {
        return new MapRecord(ADDRESS_SCHEMA, Map.of("street_address", street, "state", state, "zip_code", zipCode));
    }

    /** Reads back exactly one row via {@link CQLExecutionService#query}, for the caller to assert on. */
    private org.apache.nifi.serialization.record.Record readBack(final String tableName, final String columns, final int id) {
        final CollectingCqlQueryCallback callback = new CollectingCqlQueryCallback();

        sessionProvider.query(
                String.format("select %s from %s.%s where id = %d", columns, keyspace, tableName, id), null, callback, QueryOverrides.NONE);

        final List<org.apache.nifi.serialization.record.Record> results = callback.getRecords();
        assertEquals(1, results.size(), () -> "Expected exactly one row back from " + tableName);
        return results.getFirst();
    }

    /**
     * The single-column type matrix: one row per (record field type, CQL column type) pairing, each writing a
     * value and reading it back.
     *
     * <p>{@code written} and {@code expected} differ for the rows that exist to prove coercion happens - a
     * value handed to the service in one Java type must come back in the type the column actually holds. Where
     * they are the same object the row is a plain round-trip.
     *
     * <p>UDTs need more than a value and a comparison, so they keep their own methods below; forcing them
     * into this shape would hide what they assert.
     */
    static Stream<Arguments> singleColumnTypes() {
        final Instant timestamp = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        final UUID timeUuid = Uuids.timeBased();
        final Date sqlDate = Date.valueOf(LocalDate.of(2024, 3, 15));
        final Time sqlTime = Time.valueOf(LocalTime.of(13, 45, 30));
        final Map<String, String> map = Map.of("key1", "value1", "key2", "value2");
        final DataType stringArray = RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType());

        return Stream.of(
                // One canonical scalar round trip, as an end-to-end smoke test of the write/read path. The
                // rest of the scalar grid, and every Flexible*Codec widening, is covered without a container
                // in FlexibleCodecTest, CharacterCodecTest, and CassandraCQLExecutionServiceBindValueTest.
                arguments("STRING -> text", "string_test", "text", RecordFieldType.STRING.getDataType(), "hello world", "hello world"),

                // CQL timestamp has millisecond resolution, so the written value is truncated up front to make
                // the round-trip comparison exact.
                arguments("TIMESTAMP -> timestamp", "timestamp_test", "timestamp", RecordFieldType.TIMESTAMP.getDataType(), timestamp, timestamp),

                // A timeuuid column requires a genuine version-1 UUID; the rejection of any other version is
                // unit-tested in CassandraCQLExecutionServiceWritePathTest.
                arguments("UUID (v1) -> timeuuid", "timeuuid_test", "timeuuid", RecordFieldType.UUID.getDataType(), timeUuid, timeUuid),

                // One representative coercion end to end: a value handed in as text must land as - and read
                // back as - the type the column actually holds.
                arguments("INT <- String", "int_from_string_test", "int", RecordFieldType.INT.getDataType(), "123456", 123456),

                // java.sql.Date/Time bind through JavaSQLDateCodec and JavaSQLTimeCodec, but reads go through the
                // driver's default DATE/TIME codecs: registering those codecs adds a way to bind the java.sql
                // type, it does not replace the driver's default for an untyped read like Row.getObject(int).
                // Only a real round trip shows the bind path and the read path stay consistent.
                arguments("DATE <- java.sql.Date", "date_coercion_test", "date", RecordFieldType.DATE.getDataType(), sqlDate, sqlDate.toLocalDate()),
                arguments("TIME <- java.sql.Time", "time_coercion_test", "time", RecordFieldType.TIME.getDataType(), sqlTime, sqlTime.toLocalTime()),

                // Collection columns: convertForCqlType's element recursion plus the server's own encoding. A
                // record ARRAY can hold a List or a Set, and each must land in the matching column type.
                arguments("ARRAY (List) -> list<text>", "array_test", "list<text>", stringArray, List.of("a", "b", "c"), List.of("a", "b", "c")),
                arguments("ARRAY (Set) -> set<text>", "set_test", "set<text>", stringArray, Set.of("a", "b", "c"), Set.of("a", "b", "c")),
                arguments("MAP -> map<text, text>", "map_test", "map<text, text>",
                        RecordFieldType.MAP.getMapDataType(RecordFieldType.STRING.getDataType()), map, map));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("singleColumnTypes")
    @DisplayName("A record field writes to its CQL column and reads back as the value the column holds")
    void testSingleColumnType(final String description, final String tableName, final String cqlColumnType,
                              final DataType dataType, final Object written, final Object expected) {
        createTable(tableName, cqlColumnType);
        final RecordSchema schema = schemaFor(new RecordField("value_field", dataType));
        final MapRecord record = new MapRecord(schema, Map.of("id", 1, "value_field", written));

        insert(tableName, record);

        assertEquals(expected, readBack(tableName, "value_field", 1).getValue("value_field"));
    }

    @Test
    @DisplayName("A record with a MAP of RECORD field writes a map of UDT values to and reads it back")
    void testMapOfAddressUserDefinedType() {
        createAddressType();
        executeDdlWithRetry(String.format("create table if not exists %s.address_map_test (id int primary key, addresses map<text, frozen<address_item>>)", keyspace));

        final RecordSchema schema = schemaFor(new RecordField("addresses",
                RecordFieldType.MAP.getMapDataType(RecordFieldType.RECORD.getRecordDataType(ADDRESS_SCHEMA))));
        final Map<String, MapRecord> addressesByName = Map.of(
                "home", address("123 Main St", "NC", 27601),
                "work", address("456 Elm St", "SC", 29401));

        insert("address_map_test", new MapRecord(schema, Map.of("id", 1, "addresses", addressesByName)));

        final Object addresses = readBack("address_map_test", "addresses", 1).getValue("addresses");
        assertInstanceOf(Map.class, addresses);
        @SuppressWarnings("unchecked")
        final Map<String, UdtValue> addressMap = (Map<String, UdtValue>) addresses;
        assertEquals("123 Main St", addressMap.get("home").getString("street_address"));
        assertEquals("456 Elm St", addressMap.get("work").getString("street_address"));
    }

    @Test
    @DisplayName("A record with a UDT nested inside another UDT writes to and reads back without any UDT-specific code")
    void testPersonWithNestedAddressUserDefinedType() {
        // Deliberately different names, casing, and nesting depth than the address_item type the collection
        // tests use: this proves the RECORD-to-UdtValue conversion is driven entirely by the driver's live UDT metadata
        // rather than any UDT-specific code, since it also has to recurse into a UDT nested inside another UDT.
        // Quoting "firstName"/"lastName"/"zipCode" preserves their camelCase spelling; CQL would otherwise fold
        // unquoted identifiers to lowercase.
        executeDdlWithRetry("create type if not exists " + keyspace + ".address (street text, state text, \"zipCode\" int)");
        executeDdlWithRetry("create type if not exists " + keyspace + ".person (\"firstName\" text, \"lastName\" text, address frozen<address>)");
        executeDdlWithRetry(String.format("create table if not exists %s.person_test (id int primary key, person frozen<person>)", keyspace));

        final RecordSchema addressSchema = new SimpleRecordSchema(List.of(
                new RecordField("street", RecordFieldType.STRING.getDataType()),
                new RecordField("state", RecordFieldType.STRING.getDataType()),
                new RecordField("zipCode", RecordFieldType.INT.getDataType())
        ));
        final RecordSchema personSchema = new SimpleRecordSchema(List.of(
                new RecordField("firstName", RecordFieldType.STRING.getDataType()),
                new RecordField("lastName", RecordFieldType.STRING.getDataType()),
                new RecordField("address", RecordFieldType.RECORD.getRecordDataType(addressSchema))
        ));
        final RecordSchema schema = schemaFor(new RecordField("person", RecordFieldType.RECORD.getRecordDataType(personSchema)));

        final MapRecord address = new MapRecord(addressSchema, Map.of(
                "street", "123 Main St",
                "state", "NC",
                "zipCode", 27601));
        final MapRecord person = new MapRecord(personSchema, Map.of(
                "firstName", "John",
                "lastName", "Doe",
                "address", address));
        final MapRecord record = new MapRecord(schema, Map.of("id", 1, "person", person));

        insert("person_test", record);

        final Object readValue = readBack("person_test", "person", 1).getValue("person");
        assertInstanceOf(UdtValue.class, readValue);
        final UdtValue personValue = (UdtValue) readValue;
        assertEquals("John", personValue.getString("firstName"));
        assertEquals("Doe", personValue.getString("lastName"));
        final UdtValue addressValue = personValue.getUdtValue("address");
        assertEquals("123 Main St", addressValue.getString("street"));
        assertEquals("NC", addressValue.getString("state"));
        assertEquals(27601, addressValue.getInt("zipCode"));
    }

    @Test
    @DisplayName("A record with an ARRAY of RECORD field writes a list of UDTs to and reads it back")
    void testArrayOfAddressUserDefinedType() {
        createAddressType();
        executeDdlWithRetry(String.format("create table if not exists %s.address_array_test (id int primary key, addresses list<frozen<address_item>>)", keyspace));

        final RecordSchema schema = schemaFor(new RecordField("addresses",
                RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.RECORD.getRecordDataType(ADDRESS_SCHEMA))));
        final List<MapRecord> addresses = List.of(address("123 Main St", "NC", 27601), address("456 Elm St", "SC", 29401));

        insert("address_array_test", new MapRecord(schema, Map.of("id", 1, "addresses", addresses)));

        final Object readValue = readBack("address_array_test", "addresses", 1).getValue("addresses");
        assertInstanceOf(List.class, readValue);
        final List<?> addressList = (List<?>) readValue;
        assertEquals(2, addressList.size());
        assertInstanceOf(UdtValue.class, addressList.get(0));
        assertEquals("123 Main St", ((UdtValue) addressList.get(0)).getString("street_address"));
        assertEquals("456 Elm St", ((UdtValue) addressList.get(1)).getString("street_address"));
    }
}
