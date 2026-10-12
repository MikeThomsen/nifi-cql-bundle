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

import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.distributed.cache.client.exception.SerializationException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers every {@code DistributedMapCacheClient} operation against an in-memory stand-in for the cluster.
 *
 * <p>Worth noting what this file is not: there is no container, no driver, and no mock of a driver type. The
 * cache depends on the {@code CQLExecutionService} interface, so a fake is a {@code Map} - which is why the
 * conditional-write paths can be tested exhaustively here rather than only in an integration suite.
 */
class CQLDistributedMapCacheTest {

    private static final Serializer<String> STRING_SERIALIZER =
            (value, out) -> out.write(value.getBytes(StandardCharsets.UTF_8));
    private static final Deserializer<String> STRING_DESERIALIZER =
            input -> input == null ? null : new String(input, StandardCharsets.UTF_8);

    /** A processor exists only so the TestRunner has something to hang the controller services off. */
    public static class NoOpProcessor extends AbstractProcessor {
        @Override
        public void onTrigger(final ProcessContext context, final ProcessSession session) {
        }
    }

    private FakeCQLExecutionService cluster;
    private CQLDistributedMapCache cache;

    @BeforeEach
    void setUp() throws Exception {
        enable("false", null);
    }

    private void enable(final String strictRemoval, final String timeToLive) throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new NoOpProcessor());

        cluster = new FakeCQLExecutionService();
        runner.addControllerService("cql", cluster);
        runner.enableControllerService(cluster);

        cache = new CQLDistributedMapCache();
        runner.addControllerService("cache", cache);
        runner.setProperty(cache, CQLDistributedMapCache.CQL_EXECUTION_SERVICE, "cql");
        runner.setProperty(cache, CQLDistributedMapCache.TABLE_NAME, "ks.cache");
        runner.setProperty(cache, CQLDistributedMapCache.STRICT_REMOVAL, strictRemoval);
        if (timeToLive != null) {
            runner.setProperty(cache, CQLDistributedMapCache.TIME_TO_LIVE, timeToLive);
        }
        runner.enableControllerService(cache);
    }

    private void put(final String key, final String value) throws IOException {
        cache.put(key, value, STRING_SERIALIZER, STRING_SERIALIZER);
    }

    private String get(final String key) throws IOException {
        return cache.get(key, STRING_SERIALIZER, STRING_DESERIALIZER);
    }

    // ---- Unconditional operations --------------------------------------------------------------------

    @Test
    @DisplayName("A value put under a key reads back as the same value")
    void testPutAndGet() throws IOException {
        put("k", "v");

        assertEquals("v", get("k"));
    }

    @Test
    @DisplayName("Reading a key that was never written returns null rather than throwing")
    void testGetMissingKey() throws IOException {
        assertNull(get("absent"));
    }

    @Test
    @DisplayName("A second put over the same key overwrites, since an unconditional CQL write is an upsert")
    void testPutOverwrites() throws IOException {
        put("k", "first");
        put("k", "second");

        assertEquals("second", get("k"));
    }

    @Test
    @DisplayName("containsKey reflects presence, and reads the key column rather than transferring the value")
    void testContainsKey() throws IOException {
        assertFalse(cache.containsKey("k", STRING_SERIALIZER));
        put("k", "v");
        assertTrue(cache.containsKey("k", STRING_SERIALIZER));

        final String existenceStatement = cluster.executedStatements().getLast().cql();
        assertTrue(existenceStatement.contains("LIMIT 1"), existenceStatement);
        assertFalse(existenceStatement.contains("cache_value"),
                "an existence check must not select the value column: " + existenceStatement);
    }

    // ---- Conditional operations, the reason this needs a lightweight transaction ----------------------

    @Test
    @DisplayName("putIfAbsent creates the entry once and refuses thereafter, leaving the original value intact")
    void testPutIfAbsent() throws IOException {
        assertTrue(cache.putIfAbsent("k", "first", STRING_SERIALIZER, STRING_SERIALIZER));
        assertFalse(cache.putIfAbsent("k", "second", STRING_SERIALIZER, STRING_SERIALIZER));

        assertEquals("first", get("k"), "the losing write must not overwrite");
    }

    @Test
    @DisplayName("putIfAbsent issues a conditional statement, which is what makes it safe under concurrency")
    void testPutIfAbsentUsesLightweightTransaction() throws IOException {
        cache.putIfAbsent("k", "v", STRING_SERIALIZER, STRING_SERIALIZER);

        assertTrue(cluster.executedStatements().getLast().cql().contains("IF NOT EXISTS"),
                "without IF NOT EXISTS this is a read-then-write race, not a compare-and-set");
    }

    @Test
    @DisplayName("getAndPutIfAbsent returns null when it created the entry, since there was no prior value")
    void testGetAndPutIfAbsentWhenAbsent() throws IOException {
        assertNull(cache.getAndPutIfAbsent("k", "v", STRING_SERIALIZER, STRING_SERIALIZER, STRING_DESERIALIZER));
        assertEquals("v", get("k"));
    }

    @Test
    @DisplayName("getAndPutIfAbsent returns the stored value when it lost, in the same round trip as the attempt")
    void testGetAndPutIfAbsentWhenPresent() throws IOException {
        put("k", "original");
        cluster.clearExecuted();

        final String existing =
                cache.getAndPutIfAbsent("k", "attempted", STRING_SERIALIZER, STRING_SERIALIZER, STRING_DESERIALIZER);

        assertEquals("original", existing);
        assertEquals("original", get("k"), "the attempted write must not have landed");
        assertEquals(1, cluster.executedStatements().stream()
                        .filter(statement -> statement.cql().startsWith("INSERT")).count(),
                "the prior value must come back with the rejected write, not from a second lookup");
    }

    // ---- Removal -------------------------------------------------------------------------------------

    @Test
    @DisplayName("Best-effort removal reports prior existence and deletes unconditionally")
    void testBestEffortRemove() throws IOException {
        put("k", "v");

        assertTrue(cache.remove("k", STRING_SERIALIZER));
        assertNull(get("k"));
        assertFalse(cache.remove("k", STRING_SERIALIZER));
    }

    @Test
    @DisplayName("Best-effort removal takes two statements, which is the race Strict Removal exists to close")
    void testBestEffortRemoveIsTwoStatements() throws IOException {
        put("k", "v");
        cluster.clearExecuted();

        cache.remove("k", STRING_SERIALIZER);

        assertEquals(2, cluster.executedStatements().size(),
                "an existence check followed by an unconditional delete");
        assertFalse(cluster.executedStatements().getLast().cql().contains("IF EXISTS"));
    }

    @Test
    @DisplayName("Strict removal answers from a single conditional delete, correct under concurrency")
    void testStrictRemove() throws Exception {
        enable("true", null);
        put("k", "v");
        cluster.clearExecuted();

        assertTrue(cache.remove("k", STRING_SERIALIZER));
        assertEquals(1, cluster.executedStatements().size());
        assertTrue(cluster.executedStatements().getLast().cql().contains("IF EXISTS"));

        assertFalse(cache.remove("k", STRING_SERIALIZER), "removing an absent entry reports false");
    }

    @ParameterizedTest(name = "strictRemoval={0}")
    @ValueSource(strings = {"true", "false"})
    @DisplayName("removeAndGet returns the value it removed regardless of the Strict Removal setting")
    void testRemoveAndGet(final String strictRemoval) throws Exception {
        enable(strictRemoval, null);
        put("k", "v");

        assertEquals("v", cache.removeAndGet("k", STRING_SERIALIZER, STRING_DESERIALIZER));
        assertNull(get("k"));
        assertNull(cache.removeAndGet("k", STRING_SERIALIZER, STRING_DESERIALIZER),
                "removing an absent entry yields no prior value");
    }

    @Test
    @DisplayName("removeAndGet reads then deletes conditionally, because CQL has no delete-and-return")
    void testRemoveAndGetIsReadThenConditionalDelete() throws IOException {
        put("k", "v");
        cluster.clearExecuted();

        cache.removeAndGet("k", STRING_SERIALIZER, STRING_DESERIALIZER);

        assertEquals(2, cluster.executedStatements().size(),
                "a read for the value, then a conditional delete for the outcome");
        assertTrue(cluster.executedStatements().getFirst().cql().startsWith("SELECT"));
        assertTrue(cluster.executedStatements().getLast().cql().contains("IF EXISTS"),
                "the delete must be conditional, or a concurrent remover leaves this reporting a value nobody removed");
    }

    // ---- Bulk operations -----------------------------------------------------------------------------

    @Test
    @DisplayName("subMap returns a value per requested key, with null for the ones that are absent")
    void testSubMap() throws IOException {
        put("a", "1");
        put("c", "3");

        final Map<String, String> values =
                cache.subMap(Set.of("a", "b", "c"), STRING_SERIALIZER, STRING_DESERIALIZER);

        assertEquals(3, values.size());
        assertEquals("1", values.get("a"));
        assertNull(values.get("b"));
        assertTrue(values.containsKey("b"), "an absent key is present in the map with a null value");
        assertEquals("3", values.get("c"));
    }

    @Test
    @DisplayName("subMap of a null key set returns null, as the contract requires")
    void testSubMapOfNull() throws IOException {
        assertNull(cache.subMap(null, STRING_SERIALIZER, STRING_DESERIALIZER));
    }

    @Test
    @DisplayName("putAll writes every entry")
    void testPutAll() throws IOException {
        cache.putAll(Map.of("a", "1", "b", "2"), STRING_SERIALIZER, STRING_SERIALIZER);

        assertEquals("1", get("a"));
        assertEquals("2", get("b"));
    }

    @Test
    @DisplayName("keySet is unsupported: enumerating every key of a CQL table is a full scan")
    void testKeySet() {
        assertThrows(UnsupportedOperationException.class, () -> cache.keySet(STRING_DESERIALIZER));
    }

    // ---- Guards and configuration --------------------------------------------------------------------

    @Test
    @DisplayName("A value over the configured maximum is rejected before anything is sent to the cluster")
    void testOversizedValueIsRejectedBeforeWriting() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new NoOpProcessor());
        cluster = new FakeCQLExecutionService();
        runner.addControllerService("cql", cluster);
        runner.enableControllerService(cluster);

        cache = new CQLDistributedMapCache();
        runner.addControllerService("cache", cache);
        runner.setProperty(cache, CQLDistributedMapCache.CQL_EXECUTION_SERVICE, "cql");
        runner.setProperty(cache, CQLDistributedMapCache.TABLE_NAME, "ks.cache");
        runner.setProperty(cache, CQLDistributedMapCache.MAX_VALUE_SIZE, "4 B");
        runner.enableControllerService(cache);

        assertThrows(SerializationException.class, () -> put("k", "far too long"));
        assertTrue(cluster.executedStatements().isEmpty(),
                "nothing should reach the cluster once the value is known to be oversized");
    }

    @Test
    @DisplayName("A configured Time To Live appends USING TTL to the statements that write")
    void testTimeToLiveIsAppliedToWrites() throws Exception {
        enable("false", "1 hour");

        put("k", "v");
        assertTrue(cluster.executedStatements().getLast().cql().endsWith("USING TTL 3600"),
                cluster.executedStatements().getLast().cql());

        cache.putIfAbsent("k2", "v", STRING_SERIALIZER, STRING_SERIALIZER);
        assertTrue(cluster.executedStatements().getLast().cql().contains("USING TTL 3600"),
                "a conditional write must expire on the same terms as an unconditional one");
    }

    @Test
    @DisplayName("With no Time To Live set, statements carry no expiry clause")
    void testNoTimeToLiveByDefault() throws IOException {
        put("k", "v");

        assertFalse(cluster.executedStatements().getLast().cql().contains("TTL"),
                cluster.executedStatements().getLast().cql());
    }

    @Test
    @DisplayName("The key and value column names are the ones configured, not hard-coded")
    void testConfiguredColumnNames() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new NoOpProcessor());
        cluster = new FakeCQLExecutionService();
        cluster.withColumns("id", "payload");
        runner.addControllerService("cql", cluster);
        runner.enableControllerService(cluster);

        cache = new CQLDistributedMapCache();
        runner.addControllerService("cache", cache);
        runner.setProperty(cache, CQLDistributedMapCache.CQL_EXECUTION_SERVICE, "cql");
        runner.setProperty(cache, CQLDistributedMapCache.TABLE_NAME, "ks.cache");
        runner.setProperty(cache, CQLDistributedMapCache.KEY_COLUMN, "id");
        runner.setProperty(cache, CQLDistributedMapCache.VALUE_COLUMN, "payload");
        runner.enableControllerService(cache);

        put("k", "v");

        final String statement = cluster.executedStatements().getLast().cql();
        assertTrue(statement.contains("(id, payload)"), statement);
        assertEquals("v", get("k"));
    }

    @Test
    @DisplayName("Closing the cache releases nothing, since the session belongs to the execution service")
    void testCloseDoesNotTouchTheExecutionService() throws IOException {
        cache.close();

        put("k", "v");
        assertEquals("v", get("k"), "the cache is still usable; close() is not a teardown here");
    }

    @Test
    @DisplayName("Values are stored as opaque bytes, so a serializer's exact output survives unchanged")
    void testArbitraryBytesRoundTrip() throws IOException {
        final byte[] awkward = {0, -1, 65, 0, 127, -128};
        final Serializer<byte[]> raw = (value, out) -> out.write(value);
        final Deserializer<byte[]> rawBack = input -> input;

        cache.put("k", awkward, STRING_SERIALIZER, raw);

        org.junit.jupiter.api.Assertions.assertArrayEquals(awkward,
                cache.get("k", STRING_SERIALIZER, rawBack));
    }
}
