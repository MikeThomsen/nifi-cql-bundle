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
package org.apache.nifi.service.cql.cache;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.distributed.cache.client.exception.SerializationException;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.service.cql.api.lookup.CqlRow;
import org.apache.nifi.service.cql.api.lookup.CqlStatementResult;
import org.apache.nifi.service.cql.api.service.CQLExecutionService;
import org.apache.nifi.service.cql.api.service.QueryOverrides;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A {@link DistributedMapCacheClient} backed by Apache Cassandra or ScyllaDB, storing each entry as one row in
 * an operator-managed table.
 *
 * <p>This service opens no connection of its own. It delegates to a configured {@link CQLExecutionService},
 * which means it inherits that service's contact points, credentials, TLS, consistency level and timeouts
 * rather than restating them, and works against either backend without knowing which is configured.
 *
 * <p>The table's key and value columns must both be CQL {@code blob}. {@link Serializer} and
 * {@link Deserializer} erase the caller's Java type to bytes before this class ever sees a value, so
 * {@code blob} is the only column type that can hold them without reinterpreting an encoding this class has no
 * contract to understand. That is also why every read here goes through
 * {@link CQLExecutionService#execute} rather than the record-oriented query path: there is no schema to
 * impose, and imposing one would only have to be undone.
 *
 * <p>The conditional operations ({@code putIfAbsent}, {@code getAndPutIfAbsent}, strict {@code remove}) rely
 * on lightweight transactions, surfaced through {@link CqlStatementResult#wasApplied()}; the operational
 * trade-offs are documented in this service's additionalDetails.md.
 */
@Tags({"cassandra", "scylladb", "cql", "map", "cache", "distributed", "key/value"})
@CapabilityDescription("Provides a DistributedMapCacheClient backed by Apache Cassandra or ScyllaDB, storing each cache "
        + "entry as a single row in an existing table whose key and value columns are both of CQL type blob. Connects "
        + "through a configured CQL Execution Service rather than opening its own session, so connection settings, "
        + "credentials and TLS are configured once and shared.")
@SeeAlso(classNames = {
        "org.apache.nifi.service.cassandra.CassandraCQLExecutionService",
        "org.apache.nifi.service.scylladb.ScyllaDBCQLExecutionService",
        "org.apache.nifi.processors.cql.PutCQLRecord",
        "org.apache.nifi.processors.cql.ExecuteCQLQueryRecord"
})
public class CQLDistributedMapCache extends AbstractControllerService implements DistributedMapCacheClient {

    public static final PropertyDescriptor CQL_EXECUTION_SERVICE = new PropertyDescriptor.Builder()
            .name("CQL Execution Service")
            .description("The CQL connection service used to reach the cluster. Connection settings, credentials, TLS "
                    + "and consistency level come from that service; this cache adds only the table it stores entries in.")
            .identifiesControllerService(CQLExecutionService.class)
            .required(true)
            .build();

    public static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("Table Name")
            .description("The table cache entries are stored in, as <keyspace>.<table> or as <table> to use the "
                    + "keyspace configured on the CQL Execution Service. The table must already exist; this service "
                    + "does not create or alter schema.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor KEY_COLUMN = new PropertyDescriptor.Builder()
            .name("Key Column Name")
            .description("The table's partition key column, which must be of CQL type blob.")
            .required(true)
            .defaultValue("cache_key")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor VALUE_COLUMN = new PropertyDescriptor.Builder()
            .name("Value Column Name")
            .description("The column cache values are stored in, which must be of CQL type blob.")
            .required(true)
            .defaultValue("cache_value")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor TIME_TO_LIVE = new PropertyDescriptor.Builder()
            .name("Time To Live")
            .description("How long a written entry survives before the cluster expires it. Leave unset for entries "
                    + "that never expire. Applies to entries this service writes; it does not change entries already "
                    + "stored.")
            .required(false)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor STRICT_REMOVAL = new PropertyDescriptor.Builder()
            .name("Strict Removal")
            .description("Whether remove() must report accurately whether an entry existed. When true, removal uses a "
                    + "lightweight transaction, which is resolved by consensus and therefore correct under concurrency "
                    + "but markedly more expensive. When false, removal reads first and then deletes unconditionally: "
                    + "cheaper, and its return value is a best-effort answer that a concurrent caller can invalidate.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor MAX_VALUE_SIZE = new PropertyDescriptor.Builder()
            .name("Maximum Value Size")
            .description("Rejects a value larger than this before it is sent, rather than letting the cluster refuse "
                    + "an oversized write or accept one large enough to harm it. Cassandra and ScyllaDB tolerate large "
                    + "blobs poorly well below their hard limits.")
            .required(true)
            .defaultValue("1 MB")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            CQL_EXECUTION_SERVICE, TABLE_NAME, KEY_COLUMN, VALUE_COLUMN, TIME_TO_LIVE, STRICT_REMOVAL, MAX_VALUE_SIZE);

    private volatile CQLExecutionService executionService;
    private volatile String table;
    private volatile String keyColumn;
    private volatile String valueColumn;
    private volatile boolean strictRemoval;
    private volatile long maxValueSizeBytes;

    // Statement text is fixed once configured, so it is built at enable time rather than per operation. The
    // driver caches prepared statements by query string, so reusing the same text is what makes that cache hit.
    private volatile String putCql;
    private volatile String putIfAbsentCql;
    private volatile String fetchCql;
    private volatile String existsCql;
    private volatile String deleteCql;
    private volatile String strictDeleteCql;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        executionService = context.getProperty(CQL_EXECUTION_SERVICE).asControllerService(CQLExecutionService.class);
        table = context.getProperty(TABLE_NAME).evaluateAttributeExpressions().getValue().trim();
        keyColumn = context.getProperty(KEY_COLUMN).evaluateAttributeExpressions().getValue().trim();
        valueColumn = context.getProperty(VALUE_COLUMN).evaluateAttributeExpressions().getValue().trim();
        strictRemoval = context.getProperty(STRICT_REMOVAL).asBoolean();
        maxValueSizeBytes = context.getProperty(MAX_VALUE_SIZE).asDataSize(org.apache.nifi.processor.DataUnit.B).longValue();

        final PropertyValue ttlProperty = context.getProperty(TIME_TO_LIVE).evaluateAttributeExpressions();
        final Duration timeToLive = ttlProperty.isSet() ? ttlProperty.asDuration() : null;
        final String usingTtl = timeToLive == null ? "" : " USING TTL " + timeToLive.toSeconds();

        putCql = "INSERT INTO " + table + " (" + keyColumn + ", " + valueColumn + ") VALUES (?, ?)" + usingTtl;
        putIfAbsentCql = "INSERT INTO " + table + " (" + keyColumn + ", " + valueColumn
                + ") VALUES (?, ?) IF NOT EXISTS" + usingTtl;
        fetchCql = "SELECT " + valueColumn + " FROM " + table + " WHERE " + keyColumn + " = ?";
        existsCql = "SELECT " + keyColumn + " FROM " + table + " WHERE " + keyColumn + " = ? LIMIT 1";
        deleteCql = "DELETE FROM " + table + " WHERE " + keyColumn + " = ?";
        strictDeleteCql = "DELETE FROM " + table + " WHERE " + keyColumn + " = ? IF EXISTS";
    }

    @OnDisabled
    public void onDisabled() {
        executionService = null;
    }

    // ---- DistributedMapCacheClient -------------------------------------------------------------------

    @Override
    public <K, V> void put(final K key, final V value, final Serializer<K> keySerializer,
                           final Serializer<V> valueSerializer) throws IOException {
        execute(putCql, serializeKey(key, keySerializer), serializeValue(value, valueSerializer));
    }

    @Override
    public <K, V> boolean putIfAbsent(final K key, final V value, final Serializer<K> keySerializer,
                                      final Serializer<V> valueSerializer) throws IOException {
        return execute(putIfAbsentCql, serializeKey(key, keySerializer), serializeValue(value, valueSerializer))
                .wasApplied();
    }

    @Override
    public <K, V> V getAndPutIfAbsent(final K key, final V value, final Serializer<K> keySerializer,
                                      final Serializer<V> valueSerializer, final Deserializer<V> valueDeserializer)
            throws IOException {
        final CqlStatementResult result =
                execute(putIfAbsentCql, serializeKey(key, keySerializer), serializeValue(value, valueSerializer));

        if (result.wasApplied()) {
            return null;
        }

        // The rejected write carries back the row that beat it - see additionalDetails.md, "Conditional writes".
        return deserialize(result.rows().getFirst().getBytes(valueColumn), valueDeserializer);
    }

    @Override
    public <K, V> V get(final K key, final Serializer<K> keySerializer, final Deserializer<V> valueDeserializer)
            throws IOException {
        final CqlStatementResult result = execute(fetchCql, serializeKey(key, keySerializer));

        return result.rows().isEmpty()
                ? null
                : deserialize(result.rows().getFirst().getBytes(valueColumn), valueDeserializer);
    }

    @Override
    public <K> boolean containsKey(final K key, final Serializer<K> keySerializer) throws IOException {
        // Selects the key column, not the value - an existence check should not transfer what it will discard.
        return !execute(existsCql, serializeKey(key, keySerializer)).rows().isEmpty();
    }

    @Override
    public <K> boolean remove(final K key, final Serializer<K> keySerializer) throws IOException {
        final ByteBuffer serializedKey = serializeKey(key, keySerializer);

        if (strictRemoval) {
            return execute(strictDeleteCql, serializedKey).wasApplied();
        }

        // Best-effort under concurrency, by design - see additionalDetails.md, "Removal".
        final boolean existed = !execute(existsCql, serializedKey).rows().isEmpty();
        execute(deleteCql, serializedKey);

        return existed;
    }

    /**
     * {@inheritDoc}
     *
     * <p><strong>Not atomic, and cannot be made so in CQL.</strong> A conditional delete reports whether it
     * applied but does not return the row it removed - there is no delete-and-return statement - so the value
     * has to be read first. A writer that changes the value between that read and the delete will see this
     * return the older of the two.
     *
     * <p>What is guaranteed: the delete itself is conditional, so a {@code null} return means the entry was
     * already gone rather than that it held nothing. Callers needing a value they can rely on having removed
     * should use {@link #remove} and treat the return as existence only.
     */
    @Override
    public <K, V> V removeAndGet(final K key, final Serializer<K> keySerializer, final Deserializer<V> valueDeserializer)
            throws IOException {
        final V previous = get(key, keySerializer, valueDeserializer);

        // Conditional regardless of Strict Removal - otherwise a concurrent remover leaves this reporting a value nobody removed.
        final boolean removed = execute(strictDeleteCql, serializeKey(key, keySerializer)).wasApplied();

        return removed ? previous : null;
    }

    @Override
    public <K, V> Map<K, V> subMap(final Set<K> keys, final Serializer<K> keySerializer,
                                   final Deserializer<V> valueDeserializer) throws IOException {
        if (keys == null) {
            return null;
        }

        // One statement per key, never an IN across partition keys - see additionalDetails.md, "Bulk operations".
        final Map<K, V> values = new HashMap<>(keys.size());
        for (final K key : keys) {
            values.put(key, get(key, keySerializer, valueDeserializer));
        }

        return values;
    }

    @Override
    public <K> Set<K> keySet(final Deserializer<K> keyDeserializer) throws IOException {
        throw new UnsupportedOperationException("This operation is too expensive on a CQL data store to be made available to DistributedMapCacheClientUsers.");
    }

    @Override
    public void close() {
        // Nothing to release: the CQL Execution Service owns the session, and its lifecycle is its own.
    }

    // ---- Helpers -------------------------------------------------------------------------------------

    private CqlStatementResult execute(final String cql, final ByteBuffer... parameters) {
        return executionService.execute(cql, List.of((Object[]) parameters), QueryOverrides.NONE);
    }

    private <K> ByteBuffer serializeKey(final K key, final Serializer<K> keySerializer) throws IOException {
        return ByteBuffer.wrap(serialize(key, keySerializer));
    }

    private <V> ByteBuffer serializeValue(final V value, final Serializer<V> valueSerializer) throws IOException {
        final byte[] serialized = serialize(value, valueSerializer);

        if (serialized.length > maxValueSizeBytes) {
            throw new SerializationException(String.format(
                    "Value serializes to %d bytes, over the configured Maximum Value Size of %d",
                    serialized.length, maxValueSizeBytes));
        }

        return ByteBuffer.wrap(serialized);
    }

    private static <T> byte[] serialize(final T value, final Serializer<T> serializer) throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        serializer.serialize(value, out);
        return out.toByteArray();
    }

    private static <T> T deserialize(final byte[] value, final Deserializer<T> deserializer) throws IOException {
        return value == null ? null : deserializer.deserialize(value);
    }
}
