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

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.net.InetSocketAddress;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import static io.github.mikethomsen.nifi.cql.api.service.AbstractCQLExecutionService.HOSTNAME_VALIDATION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Covers Hostname Validation where it takes effect: on the TLS engine the driver is handed for a node. An
 * engine checks the certificate against the host only when it carries an endpoint identification algorithm.
 */
class CassandraCQLExecutionServiceHostnameValidationTest {

    @DisplayName("The TLS engine for a node checks the certificate's host only when Hostname Validation is on")
    @ParameterizedTest(name = "hostname validation={0}, endpoint identification algorithm={1}")
    @CsvSource(value = {"true,HTTPS", "false,"})
    void testEngineIdentifiesEndpointOnlyWhenValidating(final boolean hostnameValidation, final String expectedAlgorithm) throws Exception {
        final EndPoint node = mock(EndPoint.class);
        when(node.resolve()).thenReturn(InetSocketAddress.createUnresolved("node1.example.com", 9042));

        final SslEngineFactory factory = CassandraCQLExecutionService.sslEngineFactory(SSLContext.getDefault(), hostnameValidation);
        final SSLEngine engine = factory.newSslEngine(node);

        assertEquals(expectedAlgorithm, engine.getSSLParameters().getEndpointIdentificationAlgorithm());
    }

    @Test
    @DisplayName("Hostname Validation is on unless it is turned off")
    void testHostnameValidationDefaultsToOn() {
        assertEquals("true", HOSTNAME_VALIDATION.getDefaultValue());
    }
}
