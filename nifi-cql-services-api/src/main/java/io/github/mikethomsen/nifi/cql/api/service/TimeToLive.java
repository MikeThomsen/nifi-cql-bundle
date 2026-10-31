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
package io.github.mikethomsen.nifi.cql.api.service;

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.time.DurationFormat;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * The rules for a CQL Time To Live, shared by every property that takes one. CQL counts a TTL in whole seconds
 * and treats 0 as no TTL at all, so anything under a second would silently mean "never expire". Cassandra and
 * ScyllaDB both reject a TTL above 20 years.
 */
public final class TimeToLive {

    /** The largest TTL Cassandra and ScyllaDB accept: 20 years of 365 days, in seconds. */
    public static final long MAX_SECONDS = 630_720_000L;

    /**
     * Validates a time period of at least one second and at most {@link #MAX_SECONDS}. A value containing Expression Language passes through
     * unvalidated; {@link #toSeconds} checks it once evaluated.
     */
    public static final Validator VALIDATOR = (subject, input, context) -> {
        final ValidationResult timePeriod = StandardValidators.TIME_PERIOD_VALIDATOR.validate(subject, input, context);
        if (!timePeriod.isValid() || (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input))) {
            return timePeriod;
        }

        final double seconds = DurationFormat.getPreciseTimeDuration(input, TimeUnit.SECONDS);
        final String problem = seconds < 1 ? "a Time To Live must be at least 1 second"
                : seconds > MAX_SECONDS ? "a Time To Live must be at most " + MAX_SECONDS + " seconds (20 years)"
                : null;
        return new ValidationResult.Builder()
                .subject(subject)
                .input(input)
                .valid(problem == null)
                .explanation(problem)
                .build();
    };

    private TimeToLive() {
    }

    /**
     * @return {@code ttl} in the whole seconds CQL expects
     * @throws IllegalArgumentException if it is under one second or over {@link #MAX_SECONDS}
     */
    public static int toSeconds(final Duration ttl) {
        if (ttl.toSeconds() < 1) {
            throw new IllegalArgumentException("A Time To Live must be at least 1 second, but was " + ttl);
        }
        if (ttl.toSeconds() > MAX_SECONDS) {
            throw new IllegalArgumentException("A Time To Live must be at most " + MAX_SECONDS + " seconds (20 years), but was " + ttl);
        }
        return (int) ttl.toSeconds();
    }
}
