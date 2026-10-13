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
package org.apache.nifi.processors.cql;

import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RecordJsonTest {

    private static Record record(final Map<String, Object> values) {
        final List<RecordField> fields = values.keySet().stream()
                .map(name -> new RecordField(name, RecordFieldType.STRING.getDataType()))
                .toList();
        return new MapRecord(new SimpleRecordSchema(fields), new LinkedHashMap<>(values));
    }

    @Test
    void testScalars() {
        final Map<String, Object> values = new LinkedHashMap<>();
        values.put("text", "hello");
        values.put("count", 42);
        values.put("big", 12345678901L);
        values.put("price", new BigDecimal("12.50"));
        values.put("flag", true);
        values.put("missing", null);

        assertEquals("{\"text\":\"hello\",\"count\":42,\"big\":12345678901,\"price\":12.50,\"flag\":true,\"missing\":null}",
                RecordJson.toJson(record(values)));
    }

    @Test
    void testFieldsFollowSchemaOrder() {
        final Map<String, Object> values = new LinkedHashMap<>();
        for (final String name : List.of("z", "m", "a", "k", "b", "q")) {
            values.put(name, name);
        }
        final Record record = new MapRecord(new SimpleRecordSchema(
                List.of("z", "m", "a", "k", "b", "q").stream().map(n -> new RecordField(n, RecordFieldType.STRING.getDataType())).toList()),
                new HashMap<>(values));

        assertEquals("{\"z\":\"z\",\"m\":\"m\",\"a\":\"a\",\"k\":\"k\",\"b\":\"b\",\"q\":\"q\"}", RecordJson.toJson(record));
    }

    @Test
    void testStringsAreEscaped() {
        assertEquals("{\"a\":\"quote \\\" slash \\\\ line\\nbreak tab\\t ctl\\u0001\"}",
                RecordJson.toJson(record(Map.of("a", "quote \" slash \\ line\nbreak tab\t ctl\u0001"))));
    }

    @Test
    void testTemporalAndUuidAreText() {
        final UUID id = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");
        final Map<String, Object> values = new LinkedHashMap<>();
        values.put("id", id);
        values.put("at", Instant.parse("2026-01-02T03:04:05Z"));
        values.put("day", LocalDate.parse("2026-01-02"));

        assertEquals("{\"id\":\"123e4567-e89b-12d3-a456-426614174000\",\"at\":\"2026-01-02T03:04:05Z\",\"day\":\"2026-01-02\"}",
                RecordJson.toJson(record(values)));
    }

    @Test
    void testBinaryIsBase64() {
        final Map<String, Object> values = new LinkedHashMap<>();
        values.put("raw", new byte[] {1, 2, 3});
        values.put("buffer", ByteBuffer.wrap(new byte[] {1, 2, 3}));

        assertEquals("{\"raw\":\"AQID\",\"buffer\":\"AQID\"}", RecordJson.toJson(record(values)));
    }

    @Test
    void testBufferIsNotConsumed() {
        final ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2, 3});
        RecordJson.toJson(record(Map.of("buffer", buffer)));

        assertEquals(3, buffer.remaining());
    }

    @Test
    void testCollectionsAndMapsNest() {
        final Map<String, Object> values = new LinkedHashMap<>();
        values.put("list", List.of(1, 2));
        values.put("array", new Object[] {"a", null});
        values.put("map", Map.of("k", List.of()));

        assertEquals("{\"list\":[1,2],\"array\":[\"a\",null],\"map\":{\"k\":[]}}", RecordJson.toJson(record(values)));
    }

    @Test
    void testNonFiniteNumbersAreText() {
        final Map<String, Object> values = new LinkedHashMap<>();
        values.put("nan", Double.NaN);
        values.put("inf", Float.POSITIVE_INFINITY);

        assertEquals("{\"nan\":\"NaN\",\"inf\":\"Infinity\"}", RecordJson.toJson(record(values)));
    }
}
