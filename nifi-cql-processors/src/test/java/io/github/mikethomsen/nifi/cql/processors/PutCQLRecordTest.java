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

package io.github.mikethomsen.nifi.cql.processors;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordFieldType;
import io.github.mikethomsen.nifi.cql.api.constants.CqlBatchType;
import io.github.mikethomsen.nifi.cql.api.constants.UpdateMethod;
import io.github.mikethomsen.nifi.cql.api.exception.QueryFailureException;
import io.github.mikethomsen.nifi.cql.api.metadata.PrimaryKeyIdentifier;
import io.github.mikethomsen.nifi.cql.api.metadata.QualifiedTableName;
import io.github.mikethomsen.nifi.cql.api.service.CQLExecutionService;
import io.github.mikethomsen.nifi.cql.api.service.WriteOverrides;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.stubbing.Stubber;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static io.github.mikethomsen.nifi.cql.processors.constants.StatementType.UPDATE_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyMap;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PutCQLRecordTest {
    // The batch type the processor resolves when Batch Statement Type is left alone. Most tests below are
    // about something else entirely - batching, TTL, primary key overrides - and only need to name the batch
    // type because it is part of the service call they verify, so they refer to this rather than repeating a
    // literal. testDefaultBatchStatementType is the one test that actually asserts what the default is.
    private static final CqlBatchType DEFAULT_BATCH_TYPE = CqlBatchType.UNLOGGED;

    // What the processor passes when Time To Live and Timestamp Field are unset: no overrides, and nulls left
    // unset, which is the Null Value Handling default.
    private static final WriteOverrides DEFAULT_OVERRIDES = new WriteOverrides(null, null, true);

    private TestRunner runner;
    private CQLExecutionService service;
    private MockRecordParser mockReader;

    @BeforeEach
    void setUp() throws Exception {
        runner = TestRunners.newTestRunner(PutCQLRecord.class);
        service = Mockito.mock(CQLExecutionService.class);
        mockReader = new MockRecordParser();
        mockReader.addSchemaField("message", RecordFieldType.STRING);
        mockReader.addSchemaField("sender", RecordFieldType.STRING);

        when(service.getIdentifier()).thenReturn("executionService");
        when(service.getTransitUrl(any(QualifiedTableName.class))).thenReturn("cassandra://message");

        runner.setProperty(PutCQLRecord.CONNECTION_PROVIDER_SERVICE, service.getIdentifier());
        runner.setProperty(PutCQLRecord.TABLE, "message");
        runner.setProperty(PutCQLRecord.RECORD_READER_FACTORY, "reader");

        runner.addControllerService("reader", mockReader);
        runner.enableControllerService(mockReader);

        runner.addControllerService(service.getIdentifier(), service);
        runner.enableControllerService(service);
    }

    private void useSetUpdate() {
        runner.setProperty(PutCQLRecord.UPDATE_KEYS, "sender");
        runner.setProperty(PutCQLRecord.STATEMENT_TYPE, UPDATE_TYPE.getValue());
    }

    @Test
    @DisplayName("Inserting records batches them into the configured batch size and reports success")
    void testInsert() {
        final int recordCount = 250;
        final int batchCount = 3;

        runner.setProperty(PutCQLRecord.BATCH_SIZE, "100");

        for (int i = 0; i < recordCount; i++) {
            mockReader.addRecord("Hello, world", "test_user");
        }

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_SUCCESS, 1);

        verify(service, times(batchCount))
                .insert(eq(new QualifiedTableName(null, "message")), anyList(), anyMap(), eq(DEFAULT_BATCH_TYPE), eq(DEFAULT_OVERRIDES));
    }

    @Test
    @DisplayName("Batch Statement Type defaults to UNLOGGED, so bulk writes do not pay for the distributed batchlog unless asked to")
    void testDefaultBatchStatementType() {
        assertEquals(DEFAULT_BATCH_TYPE.name(), PutCQLRecord.BATCH_STATEMENT_TYPE.getDefaultValue());
    }

    @DisplayName("Updating records with each update method and batch statement type produces the expected batched calls")
    @ParameterizedTest
    @CsvSource({ "DECREMENT,COUNTER", "INCREMENT,COUNTER", "SET,LOGGED" })
    void testUpdate(UpdateMethod updateMethod, String batchStatementType) {
        useSetUpdate();
        runner.setProperty(PutCQLRecord.UPDATE_METHOD, updateMethod.name());
        runner.setProperty(PutCQLRecord.BATCH_STATEMENT_TYPE, batchStatementType);

        final int recordCount = 250;
        final int batchCount = 3;

        runner.setProperty(PutCQLRecord.BATCH_SIZE, "100");

        for (int i = 0; i < recordCount; i++) {
            mockReader.addRecord("Hello, world", "test_user");
        }

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_SUCCESS, 1);

        verify(service, times(batchCount))
                .update(eq(new QualifiedTableName(null, "message")), anyList(), anyMap(), eq(List.of("sender")), eq(updateMethod),
                        eq(CqlBatchType.valueOf(batchStatementType)), eq(DEFAULT_OVERRIDES));
    }

    @Test
    @DisplayName("An update method resolved from a FlowFile attribute is matched case-insensitively")
    void testUpdateMethodFromAttributeIsCaseInsensitive() {
        useSetUpdate();
        runner.setProperty(PutCQLRecord.UPDATE_METHOD, "USE_ATTR");

        mockReader.addRecord("Hello, world", "test_user");

        runner.enqueue("", Map.of(PutCQLRecord.UPDATE_METHOD_ATTRIBUTE, "set"));
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_SUCCESS, 1);

        verify(service, times(1))
                .update(eq(new QualifiedTableName(null, "message")), anyList(), anyMap(), eq(List.of("sender")), eq(UpdateMethod.SET), eq(DEFAULT_BATCH_TYPE), eq(DEFAULT_OVERRIDES));
    }

    static Stream<Arguments> writeOverrides() {
        return Stream.of(
                arguments("Time To Live on insert", false, PutCQLRecord.TTL, "1 hour", new WriteOverrides(Duration.ofHours(1), null, true)),
                arguments("Time To Live on update", true, PutCQLRecord.TTL, "30 min", new WriteOverrides(Duration.ofMinutes(30), null, true)),
                arguments("Timestamp Field on insert", false, PutCQLRecord.TIMESTAMP_FIELD, "sender", new WriteOverrides(null, "sender", true)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("writeOverrides")
    @DisplayName("Time To Live and Timestamp Field set on the processor are passed to the service as write overrides")
    void testWriteOverridesArePassedToService(final String description, final boolean update,
                                              final PropertyDescriptor property, final String value, final WriteOverrides expected) {
        if (update) {
            useSetUpdate();
        }
        runner.setProperty(property, value);

        mockReader.addRecord("Hello, world", "test_user");

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_SUCCESS, 1);

        final QualifiedTableName table = new QualifiedTableName(null, "message");
        if (update) {
            verify(service).update(eq(table), anyList(), anyMap(), eq(List.of("sender")), eq(UpdateMethod.SET), eq(DEFAULT_BATCH_TYPE), eq(expected));
        } else {
            verify(service).insert(eq(table), anyList(), anyMap(), eq(DEFAULT_BATCH_TYPE), eq(expected));
        }
    }

    @ParameterizedTest(name = "Null Value Handling={0}, unset nulls={1}")
    @DisplayName("Null Value Handling is passed to the service, leaving nulls unset unless told to write them")
    @CsvSource({",true", "UNSET,true", "NULL,false"})
    void testNullValueHandlingIsPassedToService(final String nullValueHandling, final boolean unsetNulls) {
        if (nullValueHandling != null) {
            runner.setProperty(PutCQLRecord.NULL_VALUE_HANDLING, nullValueHandling);
        }

        mockReader.addRecord("Hello, world", "test_user");

        runner.enqueue("");
        runner.run();

        verify(service, times(1))
                .insert(eq(new QualifiedTableName(null, "message")), anyList(), anyMap(), eq(DEFAULT_BATCH_TYPE), eq(new WriteOverrides(null, null, unsetNulls)));
    }

    @ParameterizedTest(name = "{0}")
    @DisplayName("A Time To Live under one second is invalid, since CQL would read it as no expiry at all, and so is one over the cluster's 20-year maximum")
    @CsvSource({"500 ms,false", "0 sec,false", "1 sec,true", "1500 ms,true", "7300 days,true", "7301 days,false"})
    void testTtlMustBeWithinCqlRange(final String ttl, final boolean valid) {
        runner.setProperty(PutCQLRecord.TTL, ttl);

        if (valid) {
            runner.assertValid();
        } else {
            runner.assertNotValid();
        }
    }

    @Test
    @DisplayName("A Time To Live attribute that is not a duration routes to failure instead of leaving the FlowFile queued")
    void testTtlFromAttributeThatDoesNotParseRoutesToFailure() {
        runner.setProperty(PutCQLRecord.TTL, "${ttl}");
        mockReader.addRecord("Hello, world", "test_user");

        runner.enqueue("", Map.of("ttl", "ten minutes"));
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_FAILURE, 1);
        runner.assertQueueEmpty();
        runner.getFlowFilesForRelationship(PutCQLRecord.REL_FAILURE).getFirst().assertAttributeEquals(PutCQLRecord.RECORDS_WRITTEN_ATTRIBUTE, "0");
        verify(service, never()).insert(any(QualifiedTableName.class), anyList(), anyMap(), any(CqlBatchType.class), any(WriteOverrides.class));
    }

    @Test
    @DisplayName("A cql.statement.type attribute that is neither INSERT nor UPDATE routes to failure instead of inserting")
    void testUnrecognizedStatementTypeAttributeRoutesToFailure() {
        runner.setProperty(PutCQLRecord.STATEMENT_TYPE, "USE_ATTR");

        mockReader.addRecord("Hello, world", "test_user");

        runner.enqueue("", Map.of("cql.statement.type", "DELETE"));
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_FAILURE, 1);
        runner.getFlowFilesForRelationship(PutCQLRecord.REL_FAILURE).getFirst().assertAttributeEquals("cql.records.written", "0");
        verify(service, never()).insert(any(QualifiedTableName.class), anyList(), anyMap(), any(CqlBatchType.class), any(WriteOverrides.class));
    }

    @Test
    @DisplayName("A missing cql.batch.statement.type attribute routes to failure reporting zero records written")
    void testMissingBatchStatementTypeAttributeReportsZeroRecordsWritten() {
        runner.setProperty(PutCQLRecord.BATCH_STATEMENT_TYPE, "USE_ATTR");

        mockReader.addRecord("Hello, world", "test_user");

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_FAILURE, 1);
        runner.getFlowFilesForRelationship(PutCQLRecord.REL_FAILURE).getFirst().assertAttributeEquals("cql.records.written", "0");
    }

    @Test
    @DisplayName("Setting Timestamp Field to a name absent from the record schema routes to failure without writing anything")
    void testTimestampFieldNotInSchemaRoutesToFailure() {
        runner.setProperty(PutCQLRecord.BATCH_SIZE, "100");
        runner.setProperty(PutCQLRecord.TIMESTAMP_FIELD, "does_not_exist");

        mockReader.addRecord("Hello, world", "test_user");

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_FAILURE, 1);

        verify(service, times(0)).insert(any(QualifiedTableName.class), anyList(), anyMap(), any(CqlBatchType.class), any(WriteOverrides.class));
    }

    @Test
    @DisplayName("Configuring INSERT with a COUNTER batch statement type fails validation up front")
    void testInsertWithCounterBatchTypeIsInvalid() {
        // Statement Type defaults to INSERT; Cassandra/ScyllaDB do not allow INSERT against counter tables,
        // so INSERT + COUNTER batch type can never succeed and should fail validation up front.
        runner.setProperty(PutCQLRecord.BATCH_STATEMENT_TYPE, "COUNTER");

        runner.assertNotValid();
    }

    @Test
    @DisplayName("An INSERT with a COUNTER batch statement type resolved from a FlowFile attribute routes to failure at runtime")
    void testInsertWithCounterBatchTypeAttributeRoutesToFailure() {
        // When Batch Statement Type is resolved from a FlowFile attribute, the invalid INSERT + COUNTER
        // combination can't be caught at validation time, so it must be rejected at runtime instead.
        runner.setProperty(PutCQLRecord.BATCH_STATEMENT_TYPE, "USE_ATTR");

        mockReader.addRecord("Hello, world", "test_user");

        runner.enqueue("", Map.of(PutCQLRecord.BATCH_STATEMENT_TYPE_ATTRIBUTE, "COUNTER"));
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_FAILURE, 1);
    }

    /**
     * The three ways an insert can fail partway through a FlowFile's batches, each of which must route to
     * failure and report how many records had already been written.
     *
     * <p>{@code batchesBeforeFailure} of zero is the "fails immediately" case, where nothing was written;
     * one means the first batch landed before the second threw. The exception type is the axis that matters:
     * {@link ProcessException} takes the generic error path and {@link IllegalArgumentException} the
     * configuration-error path (which is how a bad primary key override surfaces), and both have to record
     * progress the same way.
     */
    static Stream<Arguments> insertFailures() {
        return Stream.of(
                arguments("fails on the first batch", new ProcessException("Test"), 0, "0"),
                arguments("fails after one batch", new ProcessException("Test"), 1, "100"),
                arguments("fails after one batch with a configuration error",
                        new IllegalArgumentException("/missing evaluated to no values."), 1, "100"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("insertFailures")
    @DisplayName("An insert failure routes the FlowFile to failure, reporting the records written before it")
    void testInsertFailureRoutesToFailureRecordingProgress(final String description, final RuntimeException failure,
                                                           final int batchesBeforeFailure, final String expectedRecordsWritten) {
        runner.setProperty(PutCQLRecord.BATCH_SIZE, "100");

        for (int i = 0; i < 250; i++) {
            mockReader.addRecord("Hello, world", "test_user");
        }

        Stubber stubber = null;
        for (int batch = 0; batch < batchesBeforeFailure; batch++) {
            stubber = stubber == null ? doNothing() : stubber.doNothing();
        }
        (stubber == null ? doThrow(failure) : stubber.doThrow(failure))
                .when(service)
                .insert(any(QualifiedTableName.class), anyList(), anyMap(), any(CqlBatchType.class), any(WriteOverrides.class));

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_FAILURE, 1);

        final MockFlowFile failedFlowFile = runner.getFlowFilesForRelationship(PutCQLRecord.REL_FAILURE).get(0);
        assertEquals(expectedRecordsWritten, failedFlowFile.getAttribute(PutCQLRecord.RECORDS_WRITTEN_ATTRIBUTE));

        // The batches that did land each left a provenance event behind; the one that threw did not, and the
        // batches after it were never attempted.
        assertEquals(batchesBeforeFailure, runner.getProvenanceEvents().size());
    }

    @ParameterizedTest(name = "update={0}")
    @ValueSource(booleans = {false, true})
    @DisplayName("A Cassandra query failure while writing routes the FlowFile to retry rather than failure")
    void testQueryFailureRoutesToRetry(final boolean update) {
        if (update) {
            useSetUpdate();
            doThrow(new QueryFailureException()).when(service)
                    .update(any(QualifiedTableName.class), anyList(), anyMap(), anyList(), any(UpdateMethod.class), any(CqlBatchType.class), any(WriteOverrides.class));
        } else {
            doThrow(new QueryFailureException()).when(service)
                    .insert(any(QualifiedTableName.class), anyList(), anyMap(), any(CqlBatchType.class), any(WriteOverrides.class));
        }

        mockReader.addRecord("Hello, world", "test_user");

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_RETRY, 1);
    }

    @Test
    @DisplayName("Each batch emits its own provenance event, and the final one reports the true total records written")
    void testProvenanceReportsRecordsWrittenPerBatchAndTotal() {
        final int recordCount = 250;

        runner.setProperty(PutCQLRecord.BATCH_SIZE, "100");

        for (int i = 0; i < recordCount; i++) {
            mockReader.addRecord("Hello, world", "test_user");
        }

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_SUCCESS, 1);

        List<ProvenanceEventRecord> events = runner.getProvenanceEvents();
        assertEquals(3, events.size());
        assertTrue(events.get(0).getDetails().contains("100 total"));
        assertTrue(events.get(1).getDetails().contains("200 total"));
        assertTrue(events.get(2).getDetails().contains("250 total"));
    }

    @Test
    @DisplayName("An invalid table name routes to failure with cql.records.written explicitly zero")
    void testInvalidTableNameReportsZeroRecordsWritten() {
        runner.setProperty(PutCQLRecord.TABLE, "not.a.valid.table.name");

        mockReader.addRecord("Hello, world", "test_user");
        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_FAILURE, 1);

        MockFlowFile failedFlowFile = runner.getFlowFilesForRelationship(PutCQLRecord.REL_FAILURE).get(0);
        assertEquals("0", failedFlowFile.getAttribute(PutCQLRecord.RECORDS_WRITTEN_ATTRIBUTE));
    }

    @Test
    @DisplayName("A dynamic property named keyspace.table.field is accepted and described in terms of that field/table/keyspace")
    void testDynamicPropertyDescriptorAcceptsQualifiedFieldName() {
        PutCQLRecord processor = new PutCQLRecord();

        PropertyDescriptor descriptor = processor.getSupportedDynamicPropertyDescriptor("my_keyspace.my_table.id_field");

        assertTrue(descriptor.isDynamic());
        assertEquals("my_keyspace.my_table.id_field", descriptor.getName());
        assertEquals("This property sets a record path to be evaluated for field id_field on table my_keyspace.my_table",
                descriptor.getDescription());
    }

    @ParameterizedTest
    @DisplayName("A dynamic property name that isn't keyspace.table.field is rejected")
    @ValueSource(strings = {
            "id_field",                          // missing keyspace and table
            "my_table.id_field",                 // missing keyspace
            "my_keyspace.my_table.extra.id_field", // too many segments
            "",                                   // empty
            "my_keyspace.my_table.",              // trailing dot, empty field
            ".my_table.id_field",                 // leading dot, empty keyspace
            "1keyspace.my_table.id_field",        // segment starting with a digit
            "my-keyspace.my_table.id_field",      // hyphen is not a valid CQL identifier character
            "my_keyspace.my_table.id field"       // whitespace inside a segment
    })
    void testDynamicPropertyDescriptorRejectsNonQualifiedName(String propertyName) {
        PutCQLRecord processor = new PutCQLRecord();

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> processor.getSupportedDynamicPropertyDescriptor(propertyName));

        // Asserted on every case rather than once in a separate test: a user who mistypes one of several
        // dynamic properties needs the message to say which one, and that is only useful if it holds for
        // every rejection rather than for the one input a standalone test happened to pick.
        assertTrue(exception.getMessage().contains(propertyName),
                "Expected the exception message to name the invalid property, but was: " + exception.getMessage());
    }

    /**
     * Each case sets Table to {@code my_keyspace.message}, or to plain {@code message} when not qualified, adds
     * some {@code keyspace.table.field} dynamic properties, and lists the field-to-RecordPath overrides that should
     * reach the service.
     *
     * <p>An unqualified Table builds a {@link QualifiedTableName} with a null keyspace, which can never equal a
     * dynamic property's required keyspace segment, so its overrides are silently never applied.
     */
    static Stream<Arguments> keyOverrides() {
        return Stream.of(
                arguments("one override on insert", true, false,
                        Map.of("my_keyspace.message.id", "/sender"), Map.of("id", "/sender")),
                arguments("several overrides on insert", true, false,
                        Map.of("my_keyspace.message.id", "/sender", "my_keyspace.message.ts", "/message"),
                        Map.of("id", "/sender", "ts", "/message")),
                arguments("keyspace and table match whatever their case", true, false,
                        Map.of("My_Keyspace.MESSAGE.id", "/sender"), Map.of("id", "/sender")),
                arguments("an override for another table is excluded", true, false,
                        Map.of("my_keyspace.other_table.id", "/sender"), Map.of()),
                arguments("an unqualified Table matches no override", false, false,
                        Map.of("my_keyspace.message.id", "/sender"), Map.of()),
                arguments("one override on update", true, true,
                        Map.of("my_keyspace.message.id", "/message"), Map.of("id", "/message")));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("keyOverrides")
    @SuppressWarnings("unchecked")
    @DisplayName("Dynamic property RecordPath overrides for the target table are passed through as primary key overrides")
    void testDynamicPropertyOverrides(final String description, final boolean qualified, final boolean update,
                                      final Map<String, String> dynamicProperties, final Map<String, String> expected) {
        runner.setProperty(PutCQLRecord.TABLE, qualified ? "my_keyspace.message" : "message");
        if (update) {
            useSetUpdate();
        }
        dynamicProperties.forEach(runner::setProperty);

        mockReader.addRecord("Hello, world", "test_user");

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(PutCQLRecord.REL_SUCCESS, 1);

        final QualifiedTableName tableName = new QualifiedTableName(qualified ? "my_keyspace" : null, "message");
        final ArgumentCaptor<Map<PrimaryKeyIdentifier, RecordPath>> captor = ArgumentCaptor.forClass(Map.class);
        if (update) {
            verify(service).update(eq(tableName), anyList(), captor.capture(),
                    eq(List.of("sender")), eq(UpdateMethod.SET), eq(DEFAULT_BATCH_TYPE), eq(DEFAULT_OVERRIDES));
        } else {
            verify(service).insert(eq(tableName), anyList(), captor.capture(), eq(DEFAULT_BATCH_TYPE), eq(DEFAULT_OVERRIDES));
        }

        final Map<String, String> actual = new HashMap<>();
        captor.getValue().forEach((key, path) -> {
            assertTrue("my_keyspace".equalsIgnoreCase(key.keyspace()), key.keyspace());
            assertTrue("message".equalsIgnoreCase(key.tableName()), key.tableName());
            actual.put(key.fieldName(), path.getPath());
        });
        assertEquals(expected, actual);
    }
}
