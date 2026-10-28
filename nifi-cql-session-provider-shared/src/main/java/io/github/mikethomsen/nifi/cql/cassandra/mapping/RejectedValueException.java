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

package io.github.mikethomsen.nifi.cql.cassandra.mapping;

/**
 * A value the bundle rejects, with a message that names the value only when value logging is on. Kept apart
 * from other conversion errors, which may quote the value whatever the setting, so the connection service can
 * pass this message on as it is.
 */
public class RejectedValueException extends IllegalArgumentException {

    /** Whether rejected values may appear in errors, from the same property that turns on value logging. */
    public static final boolean VALUE_LOGGING_ENABLED = Boolean.getBoolean("ENABLE_CQL_VALUE_LOGGING");

    public RejectedValueException(final String message, final Throwable cause) {
        // A parser's own error repeats the text it could not parse, so it is kept only when values may be shown.
        super(message, VALUE_LOGGING_ENABLED ? cause : null);
    }

    /** The value, quoted and with a leading space, for an error message when value logging is on; otherwise nothing. */
    public static String shown(final Object value) {
        return VALUE_LOGGING_ENABLED ? " '" + value + "'" : "";
    }
}
