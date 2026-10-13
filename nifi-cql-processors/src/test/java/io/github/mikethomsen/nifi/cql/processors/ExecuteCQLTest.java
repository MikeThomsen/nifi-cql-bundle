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

import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import io.github.mikethomsen.nifi.cql.api.constants.CqlStatementType;
import io.github.mikethomsen.nifi.cql.api.exception.QueryFailureException;
import io.github.mikethomsen.nifi.cql.api.lookup.CqlStatementResult;
import io.github.mikethomsen.nifi.cql.api.service.CQLExecutionService;
import io.github.mikethomsen.nifi.cql.api.service.CQLQueryCallback;
import io.github.mikethomsen.nifi.cql.api.service.QueryOverrides;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ExecuteCQLTest {

    private static final String SELECT = "select id, name from person where id = ?";
    private static final String UPDATE = "update person set name = ? where id = ?";

    private static final RecordSchema SCHEMA = new SimpleRecordSchema(List.of(
            new RecordField("id", RecordFieldType.INT.getDataType()),
            new RecordField("name", RecordFieldType.STRING.getDataType())));

    private TestRunner runner;
    private CQLExecutionService service;

    @BeforeEach
    void setUp() throws Exception {
        runner = TestRunners.newTestRunner(ExecuteCQL.class);
        service = Mockito.mock(CQLExecutionService.class);
        when(service.getIdentifier()).thenReturn("executionService");

        runner.addControllerService(service.getIdentifier(), service);
        runner.enableControllerService(service);
        runner.setProperty(ExecuteCQL.CONNECTION_PROVIDER_SERVICE, service.getIdentifier());
        runner.setProperty(ExecuteCQL.CQL_STATEMENT, SELECT);
        runner.setProperty("cql.arg.1", "${id}");
    }

    /** Feeds {@code rowCount} rows to the callback the way a real service does, then optionally fails. */
    private void returnRows(final int rowCount, final boolean thenFail) {
        doAnswer(invocation -> {
            final CQLQueryCallback callback = invocation.getArgument(2);
            for (int row = 1; row <= rowCount; row++) {
                callback.receive(row, new MapRecord(SCHEMA, Map.of("id", row, "name", "n" + row)), thenFail || row < rowCount);
            }
            if (thenFail) {
                callback.clear();
                throw new QueryFailureException();
            }
            return null;
        }).when(service).query(anyString(), anyList(), any(CQLQueryCallback.class), any(QueryOverrides.class));
    }

    @Test
    @DisplayName("A select writes newline-delimited JSON and routes the incoming FlowFile to original")
    void testSelect() {
        returnRows(3, false);

        runner.enqueue("trigger", Map.of("id", "7"));
        runner.run();

        runner.assertTransferCount(ExecuteCQL.REL_SUCCESS, 1);
        runner.assertTransferCount(ExecuteCQL.REL_ORIGINAL, 1);

        final MockFlowFile result = runner.getFlowFilesForRelationship(ExecuteCQL.REL_SUCCESS).getFirst();
        result.assertContentEquals("{\"id\":1,\"name\":\"n1\"}\n{\"id\":2,\"name\":\"n2\"}\n{\"id\":3,\"name\":\"n3\"}\n");
        result.assertAttributeEquals("mime.type", "application/x-ndjson");
        result.assertAttributeEquals("record.count", "3");
        result.assertAttributeEquals("fragment.count", "1");

        verify(service).validateStatement(SELECT, CqlStatementType.SELECT, 1);
        final ArgumentCaptor<List<Object>> parameters = ArgumentCaptor.forClass(List.class);
        final ArgumentCaptor<QueryOverrides> overrides = ArgumentCaptor.forClass(QueryOverrides.class);
        verify(service).query(eq(SELECT), parameters.capture(), any(CQLQueryCallback.class), overrides.capture());
        assertEquals(List.of("7"), parameters.getValue());
        assertEquals(1000, overrides.getValue().fetchSize());
    }

    @Test
    @DisplayName("Rows Per FlowFile splits the result set, and fragment.count is set when nothing is released early")
    void testRowsPerFlowFile() {
        returnRows(5, false);
        runner.setProperty(ExecuteCQL.ROWS_PER_FLOW_FILE, "2");

        runner.enqueue("", Map.of("id", "7"));
        runner.run();

        runner.assertTransferCount(ExecuteCQL.REL_SUCCESS, 3);
        final List<MockFlowFile> results = runner.getFlowFilesForRelationship(ExecuteCQL.REL_SUCCESS);
        assertEquals(List.of("2", "2", "1"), results.stream().map(f -> f.getAttribute("record.count")).toList());
        assertEquals(List.of("0", "1", "2"), results.stream().map(f -> f.getAttribute("fragment.index")).toList());
        assertTrue(results.stream().allMatch(f -> "3".equals(f.getAttribute("fragment.count"))));
    }

    @Test
    @DisplayName("Minimum FlowFiles Queued releases output before the query ends, so fragment.count is left unset")
    void testMinimumQueued() {
        returnRows(5, false);
        runner.setProperty(ExecuteCQL.ROWS_PER_FLOW_FILE, "1");
        runner.setProperty(ExecuteCQL.MIN_FLOW_FILES_QUEUED, "2");

        runner.enqueue("", Map.of("id", "7"));
        runner.run();

        runner.assertTransferCount(ExecuteCQL.REL_SUCCESS, 5);
        runner.assertTransferCount(ExecuteCQL.REL_ORIGINAL, 1);
        assertNull(runner.getFlowFilesForRelationship(ExecuteCQL.REL_SUCCESS).getFirst().getAttribute("fragment.count"));
    }

    @Test
    @DisplayName("A select that returns no rows sends the incoming FlowFile to original and makes no output")
    void testNoRows() {
        returnRows(0, false);

        runner.enqueue("", Map.of("id", "7"));
        runner.run();

        runner.assertTransferCount(ExecuteCQL.REL_SUCCESS, 0);
        runner.assertTransferCount(ExecuteCQL.REL_ORIGINAL, 1);
    }

    @Test
    @DisplayName("A cluster error with nothing released discards the output and retries the incoming FlowFile")
    void testClusterErrorBeforeRelease() {
        returnRows(3, true);

        runner.enqueue("", Map.of("id", "7"));
        runner.run();

        runner.assertTransferCount(ExecuteCQL.REL_SUCCESS, 0);
        runner.assertTransferCount(ExecuteCQL.REL_RETRY, 1);
        final MockFlowFile retried = runner.getFlowFilesForRelationship(ExecuteCQL.REL_RETRY).getFirst();
        assertTrue(retried.isPenalized());
        retried.assertAttributeEquals("cql.rows.sent", "0");
        retried.assertAttributeEquals("id", "7");
    }

    @Test
    @DisplayName("A cluster error after output was released retries a replacement carrying the rows-sent count")
    void testClusterErrorAfterRelease() {
        returnRows(5, true);
        runner.setProperty(ExecuteCQL.ROWS_PER_FLOW_FILE, "2");
        runner.setProperty(ExecuteCQL.MIN_FLOW_FILES_QUEUED, "1");

        runner.enqueue("", Map.of("id", "7"));
        runner.run();

        runner.assertTransferCount(ExecuteCQL.REL_SUCCESS, 2);
        runner.assertTransferCount(ExecuteCQL.REL_ORIGINAL, 1);
        runner.assertTransferCount(ExecuteCQL.REL_RETRY, 1);
        final MockFlowFile retried = runner.getFlowFilesForRelationship(ExecuteCQL.REL_RETRY).getFirst();
        retried.assertAttributeEquals("cql.rows.sent", "4");
        retried.assertAttributeEquals("id", "7");
    }

    @Test
    @DisplayName("An update runs the statement with its parameters and reports the outcome in attributes")
    void testUpdate() {
        final CqlStatementResult result = Mockito.mock(CqlStatementResult.class);
        when(result.wasApplied()).thenReturn(true);
        when(service.execute(anyString(), anyList(), any(QueryOverrides.class))).thenReturn(result);

        runner.setProperty(ExecuteCQL.STATEMENT_TYPE, ExecuteCQL.TYPE_UPDATE);
        runner.setProperty(ExecuteCQL.CQL_STATEMENT, UPDATE);
        runner.setProperty("cql.arg.1", "${name}");
        runner.setProperty("cql.arg.2", "${id}");

        runner.enqueue("keep me", Map.of("id", "7", "name", "Ada"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteCQL.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ExecuteCQL.REL_SUCCESS).getFirst();
        out.assertContentEquals("keep me");
        out.assertAttributeEquals("cql.statement.type", "UPDATE");
        out.assertAttributeEquals("cql.applied", "true");

        verify(service).validateStatement(UPDATE, CqlStatementType.UPDATE, 2);
        verify(service).execute(eq(UPDATE), eq(List.of("Ada", "7")), any(QueryOverrides.class));
    }

    @Test
    @DisplayName("A conditional write that did not apply is reported as cql.applied=false, not as a failure")
    void testNotApplied() {
        final CqlStatementResult result = Mockito.mock(CqlStatementResult.class);
        when(result.wasApplied()).thenReturn(false);
        when(service.execute(anyString(), anyList(), any(QueryOverrides.class))).thenReturn(result);

        runner.setProperty(ExecuteCQL.STATEMENT_TYPE, ExecuteCQL.TYPE_INSERT);
        runner.setProperty(ExecuteCQL.CQL_STATEMENT, "insert into person (id) values (?) if not exists");

        runner.enqueue("", Map.of("id", "7"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteCQL.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(ExecuteCQL.REL_SUCCESS).getFirst().assertAttributeEquals("cql.applied", "false");
    }

    @Test
    @DisplayName("A statement the service rejects as invalid routes to failure and is never run")
    void testInvalidStatement() {
        doThrow(new IllegalArgumentException("no such table")).when(service).validateStatement(anyString(), any(), anyInt());

        runner.enqueue("", Map.of("id", "7"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteCQL.REL_FAILURE, 1);
        verify(service, never()).query(anyString(), anyList(), any(CQLQueryCallback.class), any(QueryOverrides.class));
    }

    @Test
    @DisplayName("A cluster error while validating routes to retry")
    void testClusterErrorWhileValidating() {
        doThrow(new QueryFailureException()).when(service).validateStatement(anyString(), any(), anyInt());

        runner.enqueue("", Map.of("id", "7"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteCQL.REL_RETRY, 1);
    }

    @Test
    @DisplayName("A write with no incoming FlowFile does nothing")
    void testWriteWithoutInput() {
        runner.setProperty(ExecuteCQL.STATEMENT_TYPE, ExecuteCQL.TYPE_DELETE);
        runner.setProperty(ExecuteCQL.CQL_STATEMENT, "delete from person where id = ?");

        runner.run();

        verify(service, never()).execute(anyString(), anyList(), any(QueryOverrides.class));
        runner.assertTransferCount(ExecuteCQL.REL_SUCCESS, 0);
    }

    @Test
    @DisplayName("The select-only properties are not needed once another statement type is chosen")
    void testSelectPropertiesDependOnSelect() {
        runner.setProperty(ExecuteCQL.STATEMENT_TYPE, ExecuteCQL.TYPE_DELETE);
        runner.setProperty(ExecuteCQL.CQL_STATEMENT, "delete from person where id = ?");
        runner.assertValid();

        // Fetch Size depends on Statement Type being Select, so the framework refuses to read it here.
        assertThrows(AssertionError.class, () -> runner.getProcessContext().getProperty(ExecuteCQL.FETCH_SIZE).getValue());
    }

    @Test
    @DisplayName("A statement of the wrong type is invalid at configuration time")
    void testWrongTypeIsInvalid() {
        runner.setProperty(ExecuteCQL.CQL_STATEMENT, "delete from person where id = ?");

        runner.assertNotValid();
    }

    @Test
    @DisplayName("An update or delete with no WHERE clause is invalid at configuration time")
    void testMissingWhereIsInvalid() {
        runner.setProperty(ExecuteCQL.STATEMENT_TYPE, ExecuteCQL.TYPE_DELETE);
        runner.setProperty(ExecuteCQL.CQL_STATEMENT, "delete from person");

        runner.assertNotValid();
    }

    @Test
    @DisplayName("A statement using Expression Language is left for run-time validation")
    void testExpressionLanguageStatementIsNotCheckedEarly() {
        runner.setProperty(ExecuteCQL.CQL_STATEMENT, "${statement}");

        runner.assertValid();
    }

    @Test
    @DisplayName("Parameter numbering with a gap is invalid")
    void testParameterGapIsInvalid() {
        runner.setProperty("cql.arg.3", "${id}");

        runner.assertNotValid();
    }

    @Test
    @DisplayName("A dynamic property that is not cql.arg.<position> is rejected")
    void testBadParameterName() {
        assertThrows(IllegalArgumentException.class, () -> runner.setProperty("id", "${id}"));
    }
}
