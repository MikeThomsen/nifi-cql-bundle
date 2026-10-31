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

import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.github.mikethomsen.nifi.cql.api.service.AbstractCQLExecutionService.CONTACT_POINTS;
import static io.github.mikethomsen.nifi.cql.api.service.AbstractCQLExecutionService.DATACENTER;
import static io.github.mikethomsen.nifi.cql.api.service.AbstractCQLExecutionService.DEFAULT_TTL;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CassandraCQLExecutionServiceDefaultTtlTest {

    @Test
    @DisplayName("A Default Time To Live from Expression Language that is out of range stops the service enabling, rather than failing every write")
    void testOutOfRangeDefaultTtlFromExpressionLanguageFailsEnabling() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(NoOpProcessor.class);
        final CassandraCQLExecutionService service = new CassandraCQLExecutionService();
        runner.addControllerService("service", service);
        runner.setProperty(service, CONTACT_POINTS, "localhost:9042");
        runner.setProperty(service, DATACENTER, "datacenter1");
        runner.setProperty(service, DEFAULT_TTL, "${ttl}");
        runner.setEnvironmentVariableValue("ttl", "30000 days");

        final Throwable thrown = assertThrows(Throwable.class, () -> runner.enableControllerService(service));

        Throwable cause = thrown;
        while (cause.getCause() != null && !(cause instanceof IllegalArgumentException)) {
            cause = cause.getCause();
        }
        assertTrue(String.valueOf(cause.getMessage()).contains("at most 630720000 seconds"), String.valueOf(cause));
    }
}
