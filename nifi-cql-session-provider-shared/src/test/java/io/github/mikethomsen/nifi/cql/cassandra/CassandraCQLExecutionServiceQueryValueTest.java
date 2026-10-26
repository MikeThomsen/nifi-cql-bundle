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

import com.datastax.oss.driver.api.core.type.DataTypes;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A tuple has no record type, so its query value is its CQL literal text. UDTs, which need the cluster's own
 * type metadata, are covered end to end in {@code AbstractCqlRecordFieldTypeIT}.
 */
public class CassandraCQLExecutionServiceQueryValueTest {

    @Test
    @DisplayName("A tuple, alone or in a list, reads back as its CQL literal rather than a driver object")
    void testTupleBecomesCqlLiteral() {
        final Object tuple = DataTypes.tupleOf(DataTypes.INT, DataTypes.TEXT).newValue(7, "x");
        final var stringArray = RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType());

        assertEquals("(7,'x')", CassandraCQLExecutionService.toRecordValue(tuple, RecordFieldType.STRING.getDataType()));
        assertEquals(List.of("(7,'x')"), CassandraCQLExecutionService.toRecordValue(List.of(tuple), stringArray));
    }
}
