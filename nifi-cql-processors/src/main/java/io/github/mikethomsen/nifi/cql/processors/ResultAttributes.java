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
package io.github.mikethomsen.nifi.cql.processors;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * The incoming FlowFile's attributes that a query's result FlowFiles carry, so results can be matched to the
 * request that produced them.
 */
final class ResultAttributes {

    /**
     * Left out because each result sets its own, or because the incoming value would describe the request rather
     * than the result: an upstream split's fragment attributes, the rows an earlier attempt had sent, and the
     * schema of the incoming content.
     */
    private static final Set<String> NOT_INHERITED = Set.of(
            CoreAttributes.UUID.key(),
            FragmentAttributes.FRAGMENT_ID.key(),
            FragmentAttributes.FRAGMENT_INDEX.key(),
            FragmentAttributes.FRAGMENT_COUNT.key(),
            "cql.rows.sent",
            "avro.schema");

    /** The prefix of the schema.name, schema.version and other attributes record readers and writers set. */
    private static final String SCHEMA_ATTRIBUTE_PREFIX = "schema.";

    private ResultAttributes() {
    }

    /** @return the attributes to copy onto each result, or none when there is no incoming FlowFile */
    static Map<String, String> inheritedFrom(final FlowFile incoming) {
        if (incoming == null) {
            return Map.of();
        }
        final Map<String, String> inherited = new HashMap<>(incoming.getAttributes());
        inherited.keySet().removeIf(name -> NOT_INHERITED.contains(name) || name.startsWith(SCHEMA_ATTRIBUTE_PREFIX));
        return Map.copyOf(inherited);
    }
}
