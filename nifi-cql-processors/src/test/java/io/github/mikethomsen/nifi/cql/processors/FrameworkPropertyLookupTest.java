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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * Guards against the HTTP 400 that NiFi 2.12.0 returned when a CQL processor's name, schedule, auto-terminated
 * relationships or other non-property setting was changed.
 *
 * <p>After every processor update, NiFi's {@code ProcessorAuditor} calls {@code getPropertyDescriptor(key)} for each
 * changed value, keyed by the labels below, to check whether the value is sensitive. {@code getPropertyDescriptor}
 * falls through to {@code getSupportedDynamicPropertyDescriptor} for any name that isn't a supported property. The
 * three CQL processors used to throw {@link IllegalArgumentException} there; the exception escaped the auditor, NiFi
 * mapped it to 400 Bad Request, and the change was applied in memory but the flow was not saved.
 */
class FrameworkPropertyLookupTest {

    /** The keys ProcessorAuditor (NiFi 2.12.0) uses for processor settings that aren't properties. */
    private static final List<String> AUDITED_SETTING_LABELS = List.of(
            "Comments", "Penalty Duration", "Yield Duration", "Concurrent Tasks", "Name", "Bulletin Level",
            "Annotation Data", "Auto Terminated Relationships", "Run Schedule", "Scheduling Strategy",
            "Execution Node", "Extension Version", "Retry Count", "Retried Relationships", "Backoff Mechanism",
            "Max Backoff Period");

    private static final List<Supplier<AbstractProcessor>> CQL_PROCESSORS = List.of(
            PutCQLRecord::new, ExecuteCQLQueryRecord::new, ExecuteCQL::new);

    static Stream<Arguments> processorsAndLabels() {
        return CQL_PROCESSORS.stream().flatMap(supplier -> AUDITED_SETTING_LABELS.stream()
                .map(label -> arguments(supplier.get().getClass().getSimpleName(), supplier, label)));
    }

    @ParameterizedTest(name = "{0}: \"{2}\"")
    @DisplayName("Looking up an audited setting label returns a descriptor instead of throwing")
    @MethodSource("processorsAndLabels")
    void auditedSettingLabelDoesNotThrow(final String processorName, final Supplier<AbstractProcessor> supplier,
                                         final String label) {
        final AbstractProcessor processor = supplier.get();

        final PropertyDescriptor descriptor = assertDoesNotThrow(() -> processor.getPropertyDescriptor(label));

        assertNotNull(descriptor);
        assertFalse(descriptor.isSensitive(), "A setting label must not be treated as a sensitive property");
    }

    @Test
    @DisplayName("Control: a processor without dynamic properties gets NiFi's fallback descriptor")
    void processorWithoutDynamicPropertiesGetsFallbackDescriptor() {
        final AbstractProcessor processor = new AbstractProcessor() {
            @Override
            public void onTrigger(final ProcessContext context, final ProcessSession session) {
            }
        };

        for (final String label : AUDITED_SETTING_LABELS) {
            final PropertyDescriptor descriptor = assertDoesNotThrow(() -> processor.getPropertyDescriptor(label));
            assertNotNull(descriptor);
        }
    }
}
