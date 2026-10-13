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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.NoNodeAvailableException;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import io.github.mikethomsen.nifi.cql.api.constants.CqlBatchType;
import io.github.mikethomsen.nifi.cql.api.constants.UpdateMethod;
import io.github.mikethomsen.nifi.cql.api.exception.QueryFailureException;
import io.github.mikethomsen.nifi.cql.api.metadata.QualifiedTableName;
import io.github.mikethomsen.nifi.cql.api.service.CQLQueryCallback;
import io.github.mikethomsen.nifi.cql.api.service.QueryOverrides;
import io.github.mikethomsen.nifi.cql.api.service.WriteOverrides;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockControllerServiceInitializationContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Covers what happens when preparing a statement fails, as distinct from running it. A node dropping while a
 * statement is prepared is a cluster error like any other and must be reported as retryable; a statement the
 * cluster rejects must not be, since no retry can fix it.
 */
class CassandraCQLExecutionServicePrepareFailureTest {

    private static final QualifiedTableName TABLE = new QualifiedTableName("ks", "message");

    private final CassandraCQLExecutionService service = new CassandraCQLExecutionService();
    private final CqlSession session = mock(CqlSession.class);

    @BeforeEach
    void setUp() throws Exception {
        service.initialize(new MockControllerServiceInitializationContext(service, "service"));
        service.setSession(session);
    }

    private static Record record() {
        final SimpleRecordSchema schema = new SimpleRecordSchema(List.of(
                new RecordField("id", RecordFieldType.STRING.getDataType()),
                new RecordField("body", RecordFieldType.STRING.getDataType())));
        return new MapRecord(schema, Map.of("id", "1", "body", "hello"));
    }

    private void prepareFailsWith(final RuntimeException failure) {
        when(session.prepare(any(SimpleStatement.class))).thenThrow(failure);
    }

    @Test
    @DisplayName("No reachable node while preparing a query is a retryable failure")
    void testQueryPrepareClusterErrorIsRetryable() {
        prepareFailsWith(new NoNodeAvailableException());
        final CQLQueryCallback callback = mock(CQLQueryCallback.class);

        assertThrows(QueryFailureException.class,
                () -> service.query("SELECT * FROM ks.message", List.of(), callback, QueryOverrides.NONE));
        // Nothing was read, so there is nothing for the callback to have built or to discard.
        verifyNoInteractions(callback);
    }

    @Test
    @DisplayName("A timeout while preparing a statement is a retryable failure")
    void testExecutePrepareTimeoutIsRetryable() {
        prepareFailsWith(new DriverTimeoutException("timed out preparing"));

        assertThrows(QueryFailureException.class,
                () -> service.execute("DELETE FROM ks.message WHERE id = ?", List.of("1"), QueryOverrides.NONE));
    }

    /** One write call against the service, named for the test report. */
    private interface Write {
        void run(CassandraCQLExecutionService service);
    }

    static Stream<Arguments> writes() {
        return Stream.of(
                arguments("insert", (Write) service -> service.insert(TABLE, record(), Map.of(), WriteOverrides.NONE)),
                arguments("batch insert", (Write) service ->
                        service.insert(TABLE, List.of(record()), Map.of(), CqlBatchType.UNLOGGED, WriteOverrides.NONE)),
                arguments("update", (Write) service ->
                        service.update(TABLE, record(), Map.of(), List.of("id"), UpdateMethod.SET, WriteOverrides.NONE)),
                arguments("batch update", (Write) service ->
                        service.update(TABLE, List.of(record()), Map.of(), List.of("id"), UpdateMethod.SET, CqlBatchType.UNLOGGED, WriteOverrides.NONE)),
                arguments("delete", (Write) service -> service.delete(TABLE, record(), Map.of(), List.of("id"))));
    }

    @DisplayName("A cluster error while preparing a write is a retryable failure")
    @ParameterizedTest(name = "{0}")
    @MethodSource("writes")
    void testWritePrepareClusterErrorIsRetryable(final String name, final Write write) {
        prepareFailsWith(new NoNodeAvailableException());

        assertThrows(QueryFailureException.class, () -> write.run(service));
    }

    @Test
    @DisplayName("A statement the cluster rejects while preparing is not reported as retryable")
    void testRejectedStatementIsNotRetryable() {
        prepareFailsWith(new InvalidQueryException(mock(Node.class), "unconfigured table no_such_table"));

        assertThrows(InvalidQueryException.class,
                () -> service.execute("DELETE FROM ks.no_such_table WHERE id = ?", List.of("1"), QueryOverrides.NONE));
    }
}
