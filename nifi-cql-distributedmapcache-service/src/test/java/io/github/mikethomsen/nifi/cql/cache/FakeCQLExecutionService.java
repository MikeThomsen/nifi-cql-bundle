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
package io.github.mikethomsen.nifi.cql.cache;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.serialization.record.Record;
import io.github.mikethomsen.nifi.cql.api.constants.CqlBatchType;
import io.github.mikethomsen.nifi.cql.api.constants.CqlStatementType;
import io.github.mikethomsen.nifi.cql.api.constants.UpdateMethod;
import io.github.mikethomsen.nifi.cql.api.lookup.CqlCell;
import io.github.mikethomsen.nifi.cql.api.lookup.CqlRow;
import io.github.mikethomsen.nifi.cql.api.lookup.CqlStatementResult;
import io.github.mikethomsen.nifi.cql.api.lookup.impl.StandardCqlCell;
import io.github.mikethomsen.nifi.cql.api.lookup.impl.StandardCqlRow;
import io.github.mikethomsen.nifi.cql.api.lookup.impl.StandardCqlStatementResult;
import io.github.mikethomsen.nifi.cql.api.metadata.PrimaryKey;
import io.github.mikethomsen.nifi.cql.api.metadata.PrimaryKeyIdentifier;
import io.github.mikethomsen.nifi.cql.api.metadata.QualifiedTableName;
import io.github.mikethomsen.nifi.cql.api.service.CQLExecutionService;
import io.github.mikethomsen.nifi.cql.api.service.CQLQueryCallback;
import io.github.mikethomsen.nifi.cql.api.service.QueryOverrides;
import io.github.mikethomsen.nifi.cql.api.service.WriteOverrides;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * An in-memory stand-in for a CQL cluster, good enough to exercise every {@code DistributedMapCacheClient}
 * operation without a container.
 *
 * <p>This is only possible because the cache depends on an interface rather than on a driver: the previous
 * driver-based implementation could be tested at this level only by mocking driver types, and could not be
 * tested at all without deciding what a {@code ResultSet} would do. Here the fake is a {@code Map}, and
 * lightweight transactions are the two lines they actually are.
 *
 * <p>It parses just enough CQL to tell the statements apart - the cache builds them itself, so recognising
 * their shape is enough, and pretending to be a parser would test the fake rather than the cache. Every
 * statement executed is recorded for tests that care how the work was done rather than what came back.
 */
class FakeCQLExecutionService extends AbstractControllerService implements CQLExecutionService {

    /** A statement as the cache issued it, for assertions about round trips and statement shape. */
    record ExecutedStatement(String cql, List<Object> parameters) {
    }

    private final Map<ByteBuffer, ByteBuffer> stored = new LinkedHashMap<>();
    private final List<ExecutedStatement> executed = new ArrayList<>();
    private String keyColumn = "cache_key";
    private String valueColumn = "cache_value";
    private RuntimeException failure;

    /** Makes every later statement throw {@code failure}, as the real service does when the cluster refuses one. */
    void failWith(final RuntimeException failure) {
        this.failure = failure;
    }

    void withColumns(final String keyColumn, final String valueColumn) {
        this.keyColumn = keyColumn;
        this.valueColumn = valueColumn;
    }

    List<ExecutedStatement> executedStatements() {
        return executed;
    }

    void clearExecuted() {
        executed.clear();
    }

    Map<ByteBuffer, ByteBuffer> stored() {
        return stored;
    }

    @Override
    public CqlStatementResult execute(final String cql, final List<Object> parameters, final QueryOverrides overrides) {
        executed.add(new ExecutedStatement(cql, parameters));
        if (failure != null) {
            throw failure;
        }

        final String normalized = cql.toUpperCase();
        final boolean conditional = normalized.contains("IF NOT EXISTS") || normalized.contains("IF EXISTS");

        if (normalized.startsWith("INSERT")) {
            return insert((ByteBuffer) parameters.get(0), (ByteBuffer) parameters.get(1), conditional);
        }
        if (normalized.startsWith("DELETE")) {
            return delete((ByteBuffer) parameters.get(0), conditional);
        }
        if (normalized.startsWith("SELECT")) {
            return select(cql, parameters);
        }

        throw new IllegalArgumentException("FakeCQLExecutionService does not recognise: " + cql);
    }

    private CqlStatementResult insert(final ByteBuffer key, final ByteBuffer value, final boolean conditional) {
        final ByteBuffer existing = stored.get(key);

        if (conditional && existing != null) {
            // The rejected write carries back the row that beat it - the behaviour a compare-and-set relies on.
            return new StandardCqlStatementResult(false, List.of(rowOf(key, existing)));
        }

        stored.put(key, value);
        return new StandardCqlStatementResult(true, List.of());
    }

    private CqlStatementResult delete(final ByteBuffer key, final boolean conditional) {
        final ByteBuffer existing = stored.remove(key);

        if (conditional) {
            return existing == null
                    ? new StandardCqlStatementResult(false, List.of())
                    : new StandardCqlStatementResult(true, List.of(rowOf(key, existing)));
        }

        return new StandardCqlStatementResult(true, List.of());
    }

    private CqlStatementResult select(final String cql, final List<Object> parameters) {
        // A full scan when unparameterized - the shape keySet() issues - and a single-key lookup otherwise.
        if (parameters.isEmpty()) {
            final List<CqlRow> rows = new ArrayList<>();
            stored.forEach((key, value) -> rows.add(rowOf(key, value)));
            return new StandardCqlStatementResult(true, rows);
        }

        final ByteBuffer key = (ByteBuffer) parameters.get(0);
        final ByteBuffer value = stored.get(key);

        return value == null
                ? new StandardCqlStatementResult(true, List.of())
                : new StandardCqlStatementResult(true, List.of(rowOf(key, value)));
    }

    private CqlRow rowOf(final ByteBuffer key, final ByteBuffer value) {
        final List<CqlCell> cells = List.of(
                new StandardCqlCell(keyColumn, key.duplicate()),
                new StandardCqlCell(valueColumn, value == null ? null : value.duplicate()));

        return new StandardCqlRow(cells);
    }

    // ---- Unused by the cache -------------------------------------------------------------------------

    @Override
    public void query(String cql, List<Object> parameters, CQLQueryCallback callback, QueryOverrides overrides) {
        throw new UnsupportedOperationException("the cache reads through execute(), never through the record API");
    }

    @Override
    public void insert(QualifiedTableName table, Record record, Map<PrimaryKeyIdentifier, RecordPath> overrides, WriteOverrides writeOverrides) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void insert(QualifiedTableName table, List<Record> records, Map<PrimaryKeyIdentifier, RecordPath> overrides, CqlBatchType batchType, WriteOverrides writeOverrides) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void update(QualifiedTableName table, Record record, Map<PrimaryKeyIdentifier, RecordPath> overrides, List<String> updateKeys, UpdateMethod updateMethod, WriteOverrides writeOverrides) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void update(QualifiedTableName table, List<Record> records, Map<PrimaryKeyIdentifier, RecordPath> overrides, List<String> updateKeys, UpdateMethod updateMethod, CqlBatchType batchType, WriteOverrides writeOverrides) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void delete(QualifiedTableName table, Record record, Map<PrimaryKeyIdentifier, RecordPath> overrides, List<String> updateKeys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void validateStatement(String cql, CqlStatementType expectedType, int parameterCount) {
        throw new UnsupportedOperationException();
    }

    @Override
    public PrimaryKey getMetadata(QualifiedTableName table) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getDefaultKeyspace() {
        return "nifi_test";
    }

    @Override
    public String getTransitUrl(QualifiedTableName tableName) {
        return "cql://fake/" + tableName;
    }
}
