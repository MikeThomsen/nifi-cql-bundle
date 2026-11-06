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
package io.github.mikethomsen.nifi.cql.processors;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import io.github.mikethomsen.nifi.cql.api.service.CQLExecutionService;

import java.util.concurrent.atomic.AtomicReference;

/**
 * AbstractCQLProcessor is a base class for Cassandra processors and contains logic and variables common to most
 * processors integrating with Apache Cassandra.
 */
public abstract class AbstractCQLProcessor extends AbstractProcessor {

    // Common descriptors
    static final PropertyDescriptor CONNECTION_PROVIDER_SERVICE = new PropertyDescriptor.Builder()
            .name("Cassandra Connection Provider")
            .description("Specifies the Cassandra connection providing controller service to be used to connect to Cassandra cluster.")
            .required(true)
            .identifiesControllerService(CQLExecutionService.class)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is transferred to this relationship if the operation completed successfully.")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is transferred to this relationship if the operation failed.")
            .build();

    static final Relationship REL_RETRY = new Relationship.Builder().name("retry")
            .description("A FlowFile is transferred to this relationship if the operation cannot be completed but attempting "
                    + "it again may succeed.")
            .build();

    protected final AtomicReference<CQLExecutionService> cqlSessionService = new AtomicReference<>(null);

    /**
     * Describes a dynamic property whose name the processor does not accept, for
     * {@code getSupportedDynamicPropertyDescriptor} to return instead of throwing.
     *
     * <p>NiFi looks up names that are not properties at all through the same method. After every processor update,
     * {@code ProcessorAuditor} asks for a descriptor for each changed setting, keyed by labels such as "Name" or
     * "Auto Terminated Relationships", to check whether it is sensitive. An exception there escapes to the REST API as
     * 400 Bad Request after the change has been applied but before the flow is saved. A descriptor that always fails
     * validation keeps those lookups working, and a mistyped dynamic property still makes the processor invalid with
     * the explanation as the reason.
     */
    static PropertyDescriptor unsupportedDynamicProperty(final String propertyDescriptorName, final String explanation) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .description(explanation)
                .dynamic(true)
                .addValidator((subject, input, context) -> new ValidationResult.Builder()
                        .subject(subject)
                        .input(input)
                        .valid(false)
                        .explanation(explanation)
                        .build())
                .build();
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        CQLExecutionService sessionProvider = context.getProperty(CONNECTION_PROVIDER_SERVICE).asControllerService(CQLExecutionService.class);
        cqlSessionService.set(sessionProvider);
    }

    public void stop(ProcessContext context) {

    }
}

