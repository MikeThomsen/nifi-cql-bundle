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

import org.apache.nifi.serialization.record.Record;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Base64;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

/**
 * Renders a {@link Record} as one compact JSON object, for the newline-delimited output of
 * {@link ExecuteCQL}. Written against the value types a CQL result row carries rather than a general JSON
 * library: numbers and booleans are bare, a binary value is Base64 text, a date/time or UUID is its ISO or
 * canonical text, collections and maps nest, and anything else is its {@code toString()}.
 */
final class RecordJson {

    private static final char[] HEX = "0123456789abcdef".toCharArray();

    private RecordJson() {
    }

    static String toJson(final Record record) {
        final StringBuilder json = new StringBuilder();
        appendRecord(json, record);
        return json.toString();
    }

    /**
     * Fields are written in schema order, which for a result row is the select-list order. A record's own map
     * makes no such promise, and a result whose keys change order between runs is needlessly hard to diff.
     */
    private static void appendRecord(final StringBuilder json, final Record record) {
        final Map<String, Object> values = record.toMap();
        json.append('{');
        boolean first = true;
        for (final String name : record.getSchema().getFieldNames()) {
            if (!first) {
                json.append(',');
            }
            first = false;
            appendString(json, name);
            json.append(':');
            appendValue(json, values.get(name));
        }
        json.append('}');
    }

    private static void appendMap(final StringBuilder json, final Map<?, ?> map) {
        json.append('{');
        boolean first = true;
        for (final Map.Entry<?, ?> entry : map.entrySet()) {
            if (!first) {
                json.append(',');
            }
            first = false;
            appendString(json, String.valueOf(entry.getKey()));
            json.append(':');
            appendValue(json, entry.getValue());
        }
        json.append('}');
    }

    private static void appendValue(final StringBuilder json, final Object value) {
        if (value == null) {
            json.append("null");
        } else if (value instanceof Boolean) {
            json.append(value);
        } else if (value instanceof Number number) {
            appendNumber(json, number);
        } else if (value instanceof byte[] bytes) {
            appendString(json, Base64.getEncoder().encodeToString(bytes));
        } else if (value instanceof ByteBuffer buffer) {
            final ByteBuffer copy = buffer.duplicate();
            final byte[] bytes = new byte[copy.remaining()];
            copy.get(bytes);
            appendString(json, Base64.getEncoder().encodeToString(bytes));
        } else if (value instanceof Date date) {
            // java.sql.Date and Time throw from toInstant(), so go through the epoch milliseconds.
            appendString(json, Instant.ofEpochMilli(date.getTime()).toString());
        } else if (value instanceof Record record) {
            appendRecord(json, record);
        } else if (value instanceof Map<?, ?> map) {
            appendMap(json, map);
        } else if (value instanceof Collection<?> collection) {
            appendArray(json, collection.toArray());
        } else if (value instanceof Object[] array) {
            appendArray(json, array);
        } else {
            appendString(json, value.toString());
        }
    }

    private static void appendArray(final StringBuilder json, final Object[] elements) {
        json.append('[');
        for (int i = 0; i < elements.length; i++) {
            if (i > 0) {
                json.append(',');
            }
            appendValue(json, elements[i]);
        }
        json.append(']');
    }

    /** JSON has no NaN or infinity, so a non-finite floating point value is written as its text. */
    private static void appendNumber(final StringBuilder json, final Number number) {
        final boolean nonFinite = (number instanceof Double d && !Double.isFinite(d)) || (number instanceof Float f && !Float.isFinite(f));
        if (nonFinite) {
            appendString(json, number.toString());
        } else {
            json.append(number);
        }
    }

    private static void appendString(final StringBuilder json, final String text) {
        json.append('"');
        for (int i = 0; i < text.length(); i++) {
            final char c = text.charAt(i);
            switch (c) {
                case '"' -> json.append("\\\"");
                case '\\' -> json.append("\\\\");
                case '\n' -> json.append("\\n");
                case '\r' -> json.append("\\r");
                case '\t' -> json.append("\\t");
                default -> {
                    if (c < 0x20) {
                        json.append("\\u00").append(HEX[c >> 4]).append(HEX[c & 0xf]);
                    } else {
                        json.append(c);
                    }
                }
            }
        }
        json.append('"');
    }
}
