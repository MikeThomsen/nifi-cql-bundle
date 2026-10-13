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

import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static io.github.mikethomsen.nifi.cql.api.service.AbstractCQLExecutionService.CONTACT_POINTS;
import static io.github.mikethomsen.nifi.cql.api.service.AbstractCQLExecutionService.DATACENTER;
import static io.github.mikethomsen.nifi.cql.api.service.AbstractCQLExecutionService.PASSWORD;
import static io.github.mikethomsen.nifi.cql.api.service.AbstractCQLExecutionService.USERNAME;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Covers validation of the credential pair. Credentials are sent only when both are present, so a service with
 * one and not the other has to be invalid rather than connect anonymously.
 */
class CassandraCQLExecutionServiceCredentialValidationTest {

    private final CassandraCQLExecutionService service = new CassandraCQLExecutionService();
    private TestRunner runner;

    @BeforeEach
    void setUp() throws Exception {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        runner.addControllerService("service", service);
        runner.setProperty(service, CONTACT_POINTS, "localhost:9042");
        runner.setProperty(service, DATACENTER, "datacenter1");
    }

    @DisplayName("Username and Password are valid together or both unset, and invalid alone")
    @ParameterizedTest(name = "username={0}, password={1}, valid={2}")
    @CsvSource({
        "cassandra,secret,true",
        ",,true",
        "cassandra,,false",
        ",secret,false"
    })
    void testCredentialsMustBeSetTogether(final String username, final String password, final boolean valid) {
        if (username != null) {
            runner.setProperty(service, USERNAME, username);
        }
        if (password != null) {
            runner.setProperty(service, PASSWORD, password);
        }

        assertEquals(valid, runner.validate(service).stream().allMatch(result -> result.isValid()));
    }
}
