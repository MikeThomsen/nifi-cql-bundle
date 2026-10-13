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

import org.apache.nifi.components.PropertyDescriptor;
import org.junit.jupiter.api.Test;

import static io.github.mikethomsen.nifi.cql.api.service.AbstractCQLExecutionService.HOSTNAME_VALIDATION;
import static io.github.mikethomsen.nifi.cql.api.service.AbstractCQLExecutionService.PROP_SSL_CONTEXT_SERVICE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers the Hostname Validation property: that the service exposes it, that it is on unless turned off, and
 * that it only applies once an SSL Context Service is set.
 */
public class CassandraCQLExecutionServiceHostnameValidationTest {

    @Test
    void testServiceExposesHostnameValidation() {
        assertTrue(new CassandraCQLExecutionService().getSupportedPropertyDescriptors().contains(HOSTNAME_VALIDATION));
    }

    @Test
    void testHostnameValidationDefaultsToOn() {
        assertEquals("true", HOSTNAME_VALIDATION.getDefaultValue());
    }

    @Test
    void testHostnameValidationDependsOnSslContextService() {
        final boolean dependsOnSslContextService = HOSTNAME_VALIDATION.getDependencies().stream()
                .anyMatch(dependency -> dependency.getPropertyName().equals(PROP_SSL_CONTEXT_SERVICE.getName()));
        assertTrue(dependsOnSslContextService);
    }
}
