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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TimeToLiveTest {

    @ParameterizedTest(name = "{0} ms is {1} s")
    @CsvSource({"1000,1", "1999,1", "86400000,86400", "630720000000,630720000"})
    void testWholeSecondsAreKept(final long millis, final int expectedSeconds) {
        assertEquals(expectedSeconds, TimeToLive.toSeconds(Duration.ofMillis(millis)));
    }

    @ParameterizedTest(name = "{0} ms")
    @CsvSource({"0", "1", "999"})
    void testUnderOneSecondIsRejected(final long millis) {
        // CQL reads a TTL of 0 as no TTL, so rounding these down would turn "expire almost at once" into "never".
        assertThrows(IllegalArgumentException.class, () -> TimeToLive.toSeconds(Duration.ofMillis(millis)));
    }

    @ParameterizedTest(name = "{0} s")
    @CsvSource({"630720001", "2592000000"})
    void testOverTwentyYearsIsRejected(final long seconds) {
        // The cluster rejects these, and the second does not even fit the int a TTL is bound as.
        assertThrows(IllegalArgumentException.class, () -> TimeToLive.toSeconds(Duration.ofSeconds(seconds)));
    }
}
