/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * See the NOTICE file distributed with this work for copyright ownership.
 */
package io.github.mikethomsen.nifi.cql.cassandra;

import com.datastax.oss.driver.api.core.type.DataTypes;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * Query values the driver decodes to something a record field cannot hold, and what they become. UDTs, which
 * need the cluster's own type metadata, are covered end to end in {@code AbstractCqlRecordFieldTypeIT}.
 */
public class CassandraCQLExecutionServiceQueryValueTest {

    private static final DataType STRING = RecordFieldType.STRING.getDataType();

    static Stream<Arguments> convertedValues() throws Exception {
        final Object tuple = DataTypes.tupleOf(DataTypes.INT, DataTypes.TEXT).newValue(7, "x");
        final InetAddress loopback = InetAddress.getByAddress(new byte[] {127, 0, 0, 1});

        return Stream.of(
                arguments("a tuple, as its CQL literal", tuple, STRING, "(7,'x')"),
                arguments("a list of tuples", List.of(tuple), RecordFieldType.ARRAY.getArrayDataType(STRING), List.of("(7,'x')")),
                arguments("a set, as a list in the cluster's order", new LinkedHashSet<>(List.of(2, 1)),
                        RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.INT.getDataType()), List.of(2, 1)),
                arguments("an inet, as its address without a leading slash", loopback, STRING, "127.0.0.1"),
                arguments("a map, with its keys as text and its values converted", Map.of(1, loopback),
                        RecordFieldType.MAP.getMapDataType(STRING), Map.of("1", "127.0.0.1")),
                arguments("a map keyed by blobs, keyed by their CQL literal", Map.of(ByteBuffer.wrap(new byte[] {(byte) 0xca, (byte) 0xfe}), "v"),
                        RecordFieldType.MAP.getMapDataType(STRING), Map.of("0xcafe", "v")));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("convertedValues")
    @DisplayName("A driver value reads back as a value its record type can hold")
    void testToRecordValue(final String description, final Object value, final DataType type, final Object expected) {
        assertEquals(expected, CassandraCQLExecutionService.toRecordValue(value, type));
    }

    @Test
    @DisplayName("A blob reads back as its bytes, not the driver's ByteBuffer")
    void testBlobBecomesBytes() {
        final DataType bytes = RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType());

        assertArrayEquals(new byte[] {(byte) 0xca, (byte) 0xfe},
                (byte[]) CassandraCQLExecutionService.toRecordValue(ByteBuffer.wrap(new byte[] {(byte) 0xca, (byte) 0xfe}), bytes));
    }
}
