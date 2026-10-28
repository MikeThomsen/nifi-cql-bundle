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
package io.github.mikethomsen.nifi.cql.cassandra;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.type.UserDefinedTypeBuilder;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.IllegalTypeConversionException;
import io.github.mikethomsen.nifi.cql.api.metadata.PrimaryKeyIdentifier;
import io.github.mikethomsen.nifi.cql.api.metadata.QualifiedTableName;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * Unit coverage for the parts of the write path that never needed a cluster: statement generation, the
 * primary-key-override field-name match, and the value conversion {@code convertForCqlType} applies before
 * {@code bind()}. Some of these started as defects found in Docker-gated ITs that had no business being
 * there; the type-coercion cases were lifted out of {@code AbstractCqlRecordFieldTypeIT} for the same
 * reason - a real container proved nothing the driver's own codecs and this class do not.
 *
 * <p>Each test asserts the <em>intended</em> behaviour, so it is a regression test the moment the code is
 * correct and fails if it regresses.
 */
public class CassandraCQLExecutionServiceWritePathTest {

    private final CassandraCQLExecutionService service = new CassandraCQLExecutionService();

    private static RecordSchema schemaOf(final RecordField... fields) {
        return new SimpleRecordSchema(List.of(fields));
    }

    // ------------------------------------------------------------------ delete() bind markers

    /**
     * Binding is {@code delete()}'s job, and it can only bind what {@code generateDelete} tells it to: the
     * ordered list of keys each of the statement's bind markers corresponds to.
     */
    @Test
    @DisplayName("generateDelete reports the keys its bind markers correspond to, so delete() can bind them")
    public void testGeneratedDeleteReportsItsBindMarkers() {
        final RecordSchema schema = schemaOf(
                new RecordField("id", RecordFieldType.INT.getDataType()),
                new RecordField("region", RecordFieldType.STRING.getDataType()),
                new RecordField("name", RecordFieldType.STRING.getDataType()));
        final Record record = new MapRecord(schema, Map.of("id", 7, "region", "us-east", "name", "seven"));

        final CassandraCQLExecutionService.GeneratedResult result =
                service.generateDelete(new QualifiedTableName("ks", "t"), record, Map.of(), List.of("id", "region"));

        final String cql = result.statement().getQuery();
        assertTrue(cql.contains(":id") && cql.contains(":region"),
                () -> "expected a bind marker per delete key, got: " + cql);

        assertEquals(List.of("id", "region"), result.keysUsed(),
                () -> "the keys backing the bind markers in: " + cql);
    }

    /**
     * A delete key resolved only through a {@code primaryKeyOverrides} RecordPath, with no same-named record
     * field, must still be accepted: {@code generateUpdate} already treats such a key as resolvable from the
     * override alone, and {@code generateDelete} must behave the same way.
     */
    @Test
    @DisplayName("A delete key supplied only by a primary key override is accepted, not rejected as missing")
    public void testGeneratedDeleteAcceptsKeyResolvedOnlyByOverride() {
        final RecordSchema schema = schemaOf(
                new RecordField("id", RecordFieldType.INT.getDataType()),
                new RecordField("created", RecordFieldType.TIMESTAMP.getDataType()));
        final Record record = new MapRecord(schema, Map.of("id", 7));

        // 'created_date' is not a record field - it exists only as a RecordPath override on this table.
        final Map<PrimaryKeyIdentifier, RecordPath> overrides = Map.of(
                new PrimaryKeyIdentifier("ks", "t", "created_date"), RecordPath.compile("/created"));

        final CassandraCQLExecutionService.GeneratedResult result =
                service.generateDelete(new QualifiedTableName("ks", "t"), record, overrides, List.of("id", "created_date"));

        assertEquals(List.of("id", "created_date"), result.keysUsed());
        assertTrue(result.statement().getQuery().contains(":created_date"),
                () -> "expected the override-resolved key to get a bind marker, got: " + result.statement().getQuery());
    }

    // ------------------------------------------------------------------ insert() bind markers

    /**
     * A primary key column supplied only through a {@code primaryKeyOverrides} RecordPath - with no field of
     * that name anywhere in the record schema - must still appear in the INSERT's column list and get a bind
     * marker. Without this, {@code generateInsert} built its columns purely from {@code schema.getFieldNames()}
     * and a derived partition/clustering column could never be written at all.
     */
    @Test
    @DisplayName("generateInsert includes a primary key override's target column even with no matching schema field")
    public void testGeneratedInsertIncludesOverrideOnlyColumn() {
        final RecordSchema schema = schemaOf(new RecordField("id", RecordFieldType.INT.getDataType()));

        // 'msg_date' is not a record field - it exists only as a RecordPath override on this table.
        final Map<PrimaryKeyIdentifier, RecordPath> overrides = Map.of(
                new PrimaryKeyIdentifier("ks", "t", "msg_date"), RecordPath.compile("/sent_at"));

        final CassandraCQLExecutionService.GeneratedResult result =
                service.generateInsert(new QualifiedTableName("ks", "t"), schema, overrides, null, null);

        assertEquals(List.of("id", "msg_date"), result.keysUsed());
        assertTrue(result.statement().getQuery().contains(":msg_date"),
                () -> "expected the override-only column to get a bind marker, got: " + result.statement().getQuery());
    }

    // ----------------------------------------------------------------- UDT null fields

    /**
     * A UDT field holding a null value has no runtime class to resolve a codec from, so it must be set via
     * {@code UdtValue.setToNull} rather than through the same codec-lookup path a non-null value uses.
     */
    @Test
    @DisplayName("A UDT with a null field converts instead of failing the codec lookup")
    public void testUdtWithNullFieldIsConvertible() {
        final UserDefinedType addressType = new UserDefinedTypeBuilder("ks", "addr")
                .withField("street", DataTypes.TEXT)
                .withField("state", DataTypes.TEXT)
                .withField("zip", DataTypes.INT)
                .build();

        final Map<String, Object> address = new HashMap<>();
        address.put("street", "1 Main St");
        address.put("state", null);
        address.put("zip", 12345);

        final Object converted = convertForCqlType(address, addressType);

        assertNotNull(converted);
        assertTrue(converted instanceof UdtValue, () -> "expected a UdtValue, got " + converted.getClass());

        final UdtValue udtValue = (UdtValue) converted;
        assertEquals("1 Main St", udtValue.getString("street"));
        assertTrue(udtValue.isNull(CqlIdentifier.fromInternal("state")), "the null field should round-trip as null");
        assertEquals(12345, udtValue.getInt("zip"));
    }

    /**
     * Same defect reached through a nested {@code Record} rather than a raw {@code Map}, since
     * {@code convertForCqlType} accepts both as representations of a UDT and a record field is the form
     * {@code PutCQLRecord} actually produces.
     */
    @Test
    @DisplayName("A UDT supplied as a nested Record with a null field converts too")
    public void testUdtSuppliedAsRecordWithNullFieldIsConvertible() {
        final UserDefinedType addressType = new UserDefinedTypeBuilder("ks", "addr")
                .withField("street", DataTypes.TEXT)
                .withField("state", DataTypes.TEXT)
                .build();

        final RecordSchema nested = schemaOf(
                new RecordField("street", RecordFieldType.STRING.getDataType()),
                new RecordField("state", RecordFieldType.STRING.getDataType()));
        final Map<String, Object> values = new HashMap<>();
        values.put("street", "1 Main St");
        values.put("state", null);

        final Object converted = convertForCqlType(new MapRecord(nested, values), addressType);

        assertTrue(converted instanceof UdtValue, () -> "expected a UdtValue, got " + converted);
        assertTrue(((UdtValue) converted).isNull(CqlIdentifier.fromInternal("state")));
    }

    // ------------------------------------------------------------------ value conversion for scalar and collection types

    /**
     * A {@code timeuuid} column only accepts a genuine version-1 UUID. The driver's own codec would reject a
     * v4 with a {@code CodecNotFoundException} that reads like a configuration fault, so {@code convertForCqlType}
     * checks first and fails with a message naming the version. Lifted out of {@code AbstractCqlRecordFieldTypeIT},
     * where a container added nothing to this check.
     */
    @Test
    @DisplayName("A non-version-1 UUID targeting a timeuuid column is rejected, naming its version but not the value")
    public void testTimeUuidRejectsNonVersion1Uuid() {
        final UUID v4 = UUID.randomUUID();

        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> convertForCqlType(v4, DataTypes.TIMEUUID));

        assertTrue(exception.getMessage().contains("version 4"), exception.getMessage());
        assertFalse(exception.getMessage().contains(v4.toString()), exception.getMessage());
    }

    /**
     * The forms a record or a parameter commonly holds for columns the driver has no codec for from them: bytes
     * as NiFi's {@code Object[]}, timestamps and numbers as text, and a collection of the other kind.
     */
    static Stream<Arguments> convertedValues() throws Exception {
        return Stream.of(
                arguments("blob from byte[]", new byte[] {1, 2}, DataTypes.BLOB, ByteBuffer.wrap(new byte[] {1, 2})),
                arguments("blob from Object[] of Byte", new Object[] {(byte) 1, (byte) 2}, DataTypes.BLOB, ByteBuffer.wrap(new byte[] {1, 2})),
                arguments("timestamp from ISO-8601 text", "2024-03-15T13:45:30Z", DataTypes.TIMESTAMP, Instant.parse("2024-03-15T13:45:30Z")),
                arguments("timestamp from epoch millis", 1710510330000L, DataTypes.TIMESTAMP, Instant.parse("2024-03-15T13:45:30Z")),
                arguments("decimal from text", "1.25", DataTypes.DECIMAL, new BigDecimal("1.25")),
                arguments("decimal from a double, by its shortest text", 1.1d, DataTypes.DECIMAL, new BigDecimal("1.1")),
                arguments("varint from a long", 5L, DataTypes.VARINT, BigInteger.valueOf(5)),
                arguments("inet from IPv4 text", "127.0.0.1", DataTypes.INET, InetAddress.getByAddress(new byte[] {127, 0, 0, 1})),
                arguments("list from Object[]", new Object[] {"a", "b"}, DataTypes.listOf(DataTypes.TEXT), List.of("a", "b")),
                arguments("list from a Set", new LinkedHashSet<>(List.of("a", "b")), DataTypes.listOf(DataTypes.TEXT), List.of("a", "b")),
                arguments("set from a List", List.of("a", "b"), DataTypes.setOf(DataTypes.TEXT), Set.of("a", "b")));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("convertedValues")
    @DisplayName("A value is converted to the Java type its column's codec binds")
    public void testConvertsForCqlType(final String description, final Object value, final DataType cqlType, final Object expected) {
        assertEquals(expected, convertForCqlType(value, cqlType));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("rejectedValues")
    @DisplayName("A value that cannot be converted is rejected without the value in the message or its cause, since value logging is off")
    public void testRejectsUnconvertibleValue(final Object value, final DataType cqlType) {
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> convertForCqlType(value, cqlType));

        assertTrue(exception.getMessage().contains(cqlType.asCql(false, true)), exception.getMessage());
        assertFalse(exception.getMessage().contains(value.toString()), exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    @DisplayName("A NiFi or driver conversion error, which can quote the value, is reduced to its type when value logging is off")
    public void testThirdPartyConversionErrorHidesTheValue() {
        final RuntimeException quoting = new IllegalTypeConversionException("Cannot convert value [40000] of type class java.lang.String to Short");

        final IllegalArgumentException exception = CassandraCQLExecutionService.conversionFailure("Field 'n' cannot be bound to its smallint column", quoting);

        assertTrue(exception.getMessage().startsWith("Field 'n' cannot be bound to its smallint column: IllegalTypeConversionException"), exception.getMessage());
        assertFalse(exception.getMessage().contains("40000"), exception.getMessage());
        assertNull(exception.getCause());
    }

    /** A host name is rejected rather than looked up; a fraction has no varint. */
    static Stream<Arguments> rejectedValues() {
        return Stream.of(arguments("localhost", DataTypes.INET), arguments(1.5d, DataTypes.VARINT), arguments("1 week", DataTypes.DURATION));
    }

    /**
     * {@code convertForCqlType} is private and has no package-visible caller that avoids a live session, so it
     * is reached reflectively rather than by widening the production API purely for a test. A
     * {@link RuntimeException} it throws (the deliberate {@code IllegalArgumentException} rejections included)
     * is re-thrown as-is so a test can assert on it; anything else fails the test naming the real cause.
     */
    private Object convertForCqlType(final Object value, final DataType cqlType) {
        try {
            final Method method = CassandraCQLExecutionService.class
                    .getDeclaredMethod("convertForCqlType", Object.class, DataType.class);
            method.setAccessible(true);
            return method.invoke(service, value, cqlType);
        } catch (final InvocationTargetException e) {
            final Throwable cause = e.getCause();
            if (cause instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            return fail("convertForCqlType threw " + cause.getClass().getName() + ": " + cause.getMessage(), cause);
        } catch (final ReflectiveOperationException e) {
            return fail("could not invoke convertForCqlType", e);
        }
    }

    // ------------------------------------------------------------------ override field-name matching

    /**
     * The override lookup must match keyspace, table and field case-insensitively, via {@code CqlIdentifier}
     * normalization: a dynamic property name naturally carries whatever case it was typed with (e.g.
     * {@code KS.T.MyField}), while what it targets is lowercase, unquoted CQL (e.g. {@code ks.t.myfield}).
     */
    @Test
    @DisplayName("A primary key override declared with mixed-case names matches the lowercase keyspace, table and column")
    public void testOverrideMatchIsCaseInsensitive() {
        final RecordPath path = RecordPath.compile("/source");
        final Map<PrimaryKeyIdentifier, RecordPath> overrides =
                Map.of(new PrimaryKeyIdentifier("KS", "T", "MyField"), path);

        final RecordPath matched = getRecordPathOverride(new QualifiedTableName("ks", "t"), "myfield", overrides);

        assertNotNull(matched, "expected the mixed-case override to match the lowercase keyspace, table and column");
        assertEquals(path, matched);
    }

    // ------------------------------------------------------------------ override evaluation

    /**
     * The value bound for an override-resolved column is whatever the override's {@code RecordPath} selects,
     * evaluated against the record. A {@code format()} override - the shape that derives a partition or
     * clustering column from a timestamp field - must yield the formatted {@link String}, which requires
     * {@code format()} to accept the {@code java.sql.Timestamp} the caller supplies for the source field.
     */
    @Test
    @DisplayName("A format() override derives the expected String from a java.sql.Timestamp source field")
    public void testEvaluateOverrideDerivesFormattedStringFromTimestamp() {
        final RecordSchema schema = schemaOf(new RecordField("sent_at", RecordFieldType.TIMESTAMP.getDataType()));
        final Record record = new MapRecord(schema,
                Map.of("sent_at", Timestamp.from(Instant.parse("2026-08-01T09:15:00Z"))));

        assertEquals("2026-08-01",
                evaluateOverride(record, RecordPath.compile("format(/sent_at, 'yyyy-MM-dd', 'UTC')")));
        assertEquals("9",
                evaluateOverride(record, RecordPath.compile("format(/sent_at, 'H', 'UTC')")));
    }

    /**
     * A RecordPath that selects nothing (a column with no matching field, and no override value to fall back
     * on) must fail rather than bind null into a primary key column.
     */
    @Test
    @DisplayName("An override whose RecordPath selects no value is rejected")
    public void testEvaluateOverrideRejectsNoValue() {
        final RecordSchema schema = schemaOf(
                new RecordField("tags", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType())));
        final Record record = new MapRecord(schema, Map.of("tags", new Object[] {"a", "b"}));

        // An array index past the end selects nothing at all.
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> evaluateOverride(record, RecordPath.compile("/tags[9]")));

        assertTrue(exception.getMessage().contains("no values"), exception.getMessage());
    }

    /**
     * A RecordPath that selects more than one value has no single value to bind, so it must fail rather than
     * pick one arbitrarily.
     */
    @Test
    @DisplayName("An override whose RecordPath selects more than one value is rejected")
    public void testEvaluateOverrideRejectsMultipleValues() {
        final RecordSchema schema = schemaOf(
                new RecordField("a", RecordFieldType.STRING.getDataType()),
                new RecordField("b", RecordFieldType.STRING.getDataType()));
        final Record record = new MapRecord(schema, Map.of("a", "x", "b", "y"));

        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> evaluateOverride(record, RecordPath.compile("/*")));

        assertTrue(exception.getMessage().contains("more than one value"), exception.getMessage());
    }

    /**
     * {@code getRecordPathOverride} is private and touches no session, so it is reached reflectively rather
     * than by widening production visibility purely for a test.
     */
    private RecordPath getRecordPathOverride(final QualifiedTableName tableName, final String fieldName,
                                             final Map<PrimaryKeyIdentifier, RecordPath> overrides) {
        try {
            final Method method = CassandraCQLExecutionService.class.getDeclaredMethod(
                    "getRecordPathOverride", QualifiedTableName.class, String.class, Map.class);
            method.setAccessible(true);
            return (RecordPath) method.invoke(service, tableName, fieldName, overrides);
        } catch (final InvocationTargetException e) {
            final Throwable cause = e.getCause();
            return fail("getRecordPathOverride threw " + cause.getClass().getName() + ": " + cause.getMessage(), cause);
        } catch (final ReflectiveOperationException e) {
            return fail("could not invoke getRecordPathOverride", e);
        }
    }

    /**
     * {@code evaluateOverride} is private and touches no session, so it is reached reflectively. A
     * {@link RuntimeException} it throws is re-thrown as-is so a test can assert on the rejection.
     */
    private Object evaluateOverride(final Record record, final RecordPath path) {
        try {
            final Method method = CassandraCQLExecutionService.class.getDeclaredMethod(
                    "evaluateOverride", Record.class, RecordPath.class);
            method.setAccessible(true);
            return method.invoke(service, record, path);
        } catch (final InvocationTargetException e) {
            final Throwable cause = e.getCause();
            if (cause instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            return fail("evaluateOverride threw " + cause.getClass().getName() + ": " + cause.getMessage(), cause);
        } catch (final ReflectiveOperationException e) {
            return fail("could not invoke evaluateOverride", e);
        }
    }
}
