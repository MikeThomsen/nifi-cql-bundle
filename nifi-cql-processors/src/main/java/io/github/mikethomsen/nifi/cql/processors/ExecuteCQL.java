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

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import io.github.mikethomsen.nifi.cql.api.constants.CqlStatementType;
import io.github.mikethomsen.nifi.cql.api.exception.QueryFailureException;
import io.github.mikethomsen.nifi.cql.api.lookup.CqlStatementResult;
import io.github.mikethomsen.nifi.cql.api.service.CQLExecutionService;
import io.github.mikethomsen.nifi.cql.api.service.CqlStatements;
import io.github.mikethomsen.nifi.cql.api.service.QueryOverrides;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.String.format;

@Tags({"cassandra", "scylladb", "cql", "select", "insert", "update", "delete", "query", "execute"})
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@CapabilityDescription("Runs a single CQL statement - a SELECT, INSERT, UPDATE or DELETE, chosen with 'Statement Type' - "
        + "against a CQL data store (Cassandra or ScyllaDB primarily). Before the statement runs it is validated: it must be "
        + "of the chosen type, an UPDATE or DELETE must have a WHERE clause, the cluster must accept it (so the keyspace, "
        + "table and columns exist), and its '?' bind markers must match the cql.arg.<position> parameters. A SELECT writes "
        + "its rows as newline-delimited JSON, one object per row, split across FlowFiles as configured; the other "
        + "statements pass the incoming FlowFile through to success. Failures of the cluster, such as a node going away, "
        + "route to retry; anything wrong with the statement or its parameters routes to failure.")
@DynamicProperty(name = "cql.arg.<position>", value = "The value to bind to that bind marker",
        expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        description = "Supplies the value for one '?' bind marker in the CQL statement, where <position> is the marker's "
                + "1-based position - cql.arg.1 for the first, cql.arg.2 for the second, and so on. Positions must run "
                + "consecutively from 1, and the number of parameters must match the number of bind markers. Each value is "
                + "sent to the cluster as data and is never parsed as CQL, so this is the safe way to build a statement "
                + "around a value taken from a FlowFile attribute.")
@WritesAttributes({
        @WritesAttribute(attribute = "cql.statement.type", description = "On an INSERT, UPDATE or DELETE, the type of statement that ran."),
        @WritesAttribute(attribute = "cql.applied", description = "On an INSERT, UPDATE or DELETE, whether the statement took effect. "
                + "Always true unless the statement is conditional (IF [NOT] EXISTS or IF <condition>) and its condition failed."),
        @WritesAttribute(attribute = "cql.rows.sent", description = "On a SELECT routed to retry, the number of rows that earlier "
                + "output had already released downstream before the failure. A retry runs the whole query again, so those rows "
                + "will be produced a second time."),
        @WritesAttribute(attribute = "mime.type", description = "On each SELECT result FlowFile, application/x-ndjson."),
        @WritesAttribute(attribute = "record.count", description = "On each SELECT result FlowFile, the number of rows in it."),
        @WritesAttribute(attribute = "fragment.identifier", description = "On each SELECT result FlowFile, an identifier shared by all FlowFiles of one query."),
        @WritesAttribute(attribute = "fragment.index", description = "On each SELECT result FlowFile, its position within the query's output."),
        @WritesAttribute(attribute = "fragment.count", description = "On each SELECT result FlowFile, the total number of FlowFiles the query produced. "
                + "Only set when 'Minimum FlowFiles Queued' is 0, because otherwise earlier FlowFiles are released before the total is known.")
})
@SupportsBatching
@SystemResourceConsideration(resource = SystemResource.MEMORY,
        description = "With 'Minimum FlowFiles Queued' of 0, every SELECT result FlowFile is held in the session until the whole "
                + "result set has been read. Set it, along with 'Rows Per FlowFile', when querying large tables.")
@SeeAlso(
        value = {PutCQLRecord.class, ExecuteCQLQueryRecord.class},
        // The session provider services cannot be referenced by class: this module is barred from depending on either
        // of them - and so on the database drivers they carry - by the ban-database-client-dependencies enforcer rule.
        classNames = {
                "io.github.mikethomsen.nifi.cql.cassandra.CassandraCQLExecutionService",
                "io.github.mikethomsen.nifi.cql.scylladb.ScyllaDBCQLExecutionService"
        })
public class ExecuteCQL extends AbstractCQLProcessor {

    static final String ATTRIBUTE_STATEMENT_TYPE = "cql.statement.type";
    static final String ATTRIBUTE_APPLIED = "cql.applied";
    static final String ATTRIBUTE_ROWS_SENT = "cql.rows.sent";

    /** Matches the dynamic property name of a positional parameter, capturing its 1-based position. */
    private static final Pattern PARAMETER_PATTERN = Pattern.compile("^cql\\.arg\\.(?<position>[1-9]\\d*)$");

    static final AllowableValue TYPE_SELECT = new AllowableValue(CqlStatementType.SELECT.name(), "Select",
            "Reads rows and writes them as newline-delimited JSON.");
    static final AllowableValue TYPE_INSERT = new AllowableValue(CqlStatementType.INSERT.name(), "Insert", "Writes a row.");
    static final AllowableValue TYPE_UPDATE = new AllowableValue(CqlStatementType.UPDATE.name(), "Update",
            "Changes rows. Must have a WHERE clause.");
    static final AllowableValue TYPE_DELETE = new AllowableValue(CqlStatementType.DELETE.name(), "Delete",
            "Removes rows or columns. Must have a WHERE clause.");

    static final PropertyDescriptor STATEMENT_TYPE = new PropertyDescriptor.Builder()
            .name("Statement Type")
            .description("The kind of statement to run. The statement text must be of this kind, or the FlowFile is routed to failure.")
            .required(true)
            .allowableValues(TYPE_SELECT, TYPE_INSERT, TYPE_UPDATE, TYPE_DELETE)
            .defaultValue(TYPE_SELECT.getValue())
            .build();

    static final PropertyDescriptor CQL_STATEMENT = new PropertyDescriptor.Builder()
            .name("CQL Statement")
            .description("""
                    The CQL statement to run. Values that come from outside the flow's own configuration - a FlowFile \
                    attribute, for example - should be supplied as '?' bind markers with matching cql.arg.<position> \
                    dynamic properties rather than interpolated into this text, since anything interpolated here is \
                    parsed as CQL.""")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor QUERY_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Max Wait Time")
            .description("""
                    The maximum amount of time allowed for this statement to run, overriding the Read Timeout configured on the \
                    connection service for this statement only. Must be of format <duration> <TimeUnit> where <duration> is a \
                    non-negative integer and TimeUnit is a supported Time Unit, such as: nanos, millis, secs, mins, hrs, days. \
                    If not set, the connection service's configured Read Timeout is used.""")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    static final PropertyDescriptor FETCH_SIZE = new PropertyDescriptor.Builder()
            .name("Fetch Size")
            .description("The number of result rows to fetch from the cluster at a time.")
            .required(true)
            .defaultValue("1000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .dependsOn(STATEMENT_TYPE, TYPE_SELECT)
            .build();

    static final PropertyDescriptor ROWS_PER_FLOW_FILE = new PropertyDescriptor.Builder()
            .name("Rows Per FlowFile")
            .description("The maximum number of rows in one result FlowFile, to break a large result set into several. "
                    + "If zero, all rows go into a single FlowFile.")
            .required(true)
            .defaultValue("0")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .dependsOn(STATEMENT_TYPE, TYPE_SELECT)
            .build();

    static final PropertyDescriptor MIN_FLOW_FILES_QUEUED = new PropertyDescriptor.Builder()
            .name("Minimum FlowFiles Queued")
            .description("""
                    The number of completed result FlowFiles to hold before releasing them downstream. Each time this many \
                    are ready they are released together, so output starts flowing while the query is still being read. \
                    If zero, nothing is released until the entire result set has been retrieved. Because a release cannot \
                    be undone, a query that fails after one will, when retried, produce those rows again.""")
            .required(true)
            .defaultValue("0")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .dependsOn(STATEMENT_TYPE, TYPE_SELECT)
            .build();

    static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .autoTerminateDefault(true)
            .name("original")
            .description("For a SELECT, the incoming FlowFile once its result FlowFiles have been released, or immediately "
                    + "if the query returned no rows. On a failed query the incoming FlowFile goes to failure or retry instead.")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            CONNECTION_PROVIDER_SERVICE, STATEMENT_TYPE, CQL_STATEMENT, QUERY_TIMEOUT,
            FETCH_SIZE, ROWS_PER_FLOW_FILE, MIN_FLOW_FILES_QUEUED);

    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS, REL_ORIGINAL, REL_FAILURE, REL_RETRY);

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        final Matcher matcher = PARAMETER_PATTERN.matcher(propertyDescriptorName);

        if (!matcher.matches()) {
            throw new IllegalArgumentException(format(
                    "'%s' is not a valid parameter name; positional parameters are named cql.arg.1, cql.arg.2, and so on",
                    propertyDescriptorName));
        }

        return new PropertyDescriptor.Builder()
                .dynamic(true)
                .name(propertyDescriptorName)
                .description(format("The value bound to bind marker %s of the CQL statement.", matcher.group("position")))
                .required(false)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
                .build();
    }

    /**
     * Rejects a gap or non-1 start in the parameter numbering, which would silently bind a marker with another
     * marker's value, and - when the statement has no Expression Language and so can be read now - a statement
     * that is not of the chosen type or is an UPDATE/DELETE with no WHERE clause.
     */
    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final List<ValidationResult> results = new ArrayList<>();

        final List<Integer> positions = context.getProperties().keySet().stream()
                .filter(PropertyDescriptor::isDynamic)
                .map(descriptor -> PARAMETER_PATTERN.matcher(descriptor.getName()))
                .filter(Matcher::matches)
                .map(matcher -> Integer.valueOf(matcher.group("position")))
                .sorted()
                .toList();

        for (int index = 0; index < positions.size(); index++) {
            if (positions.get(index) != index + 1) {
                results.add(new ValidationResult.Builder()
                        .subject("Query parameters")
                        .valid(false)
                        .explanation(format("positional parameters must be numbered consecutively starting at 1, but cql.arg.%d is missing", index + 1))
                        .build());
                break;
            }
        }

        final PropertyValue statement = context.getProperty(CQL_STATEMENT);
        if (statement.isSet() && !context.isExpressionLanguagePresent(statement.getValue())) {
            final String problem = textProblem(statement.getValue(), CqlStatementType.valueOf(context.getProperty(STATEMENT_TYPE).getValue()));
            if (problem != null) {
                results.add(new ValidationResult.Builder().subject(CQL_STATEMENT.getDisplayName()).valid(false).explanation(problem).build());
            }
        }

        return results;
    }

    /** @return what is wrong with the statement text for {@code expected}, found without a cluster, or {@code null} */
    private static String textProblem(final String cql, final CqlStatementType expected) {
        final CqlStatementType actual = CqlStatements.typeOf(cql);
        if (actual != expected) {
            return format("the statement is %s but Statement Type is %s", actual == null ? "not a SELECT, INSERT, UPDATE or DELETE" : actual, expected);
        }
        if ((expected == CqlStatementType.UPDATE || expected == CqlStatementType.DELETE) && !CqlStatements.hasWhereClause(cql)) {
            return format("a %s without a WHERE clause is not allowed", expected);
        }
        return null;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = null;

        if (context.hasIncomingConnection()) {
            flowFile = session.get();

            // With nothing queued and a real upstream connection, there is nothing to do; only a self-loop
            // (or no connection) lets a SELECT run as a source.
            if (flowFile == null && context.hasNonLoopConnection()) {
                return;
            }
        }

        final CqlStatementType type = CqlStatementType.valueOf(context.getProperty(STATEMENT_TYPE).getValue());

        if (type != CqlStatementType.SELECT && flowFile == null) {
            // Nothing identifies the row to write, and a write with no trigger would repeat on every run.
            getLogger().warn("{} statements need an incoming FlowFile; yielding", type);
            context.yield();
            return;
        }

        final CQLExecutionService service = cqlSessionService.get();
        final String cql = context.getProperty(CQL_STATEMENT).evaluateAttributeExpressions(flowFile).getValue();

        final PropertyValue timeoutProperty = context.getProperty(QUERY_TIMEOUT).evaluateAttributeExpressions(flowFile);
        final Duration timeout = timeoutProperty.isSet() ? timeoutProperty.asDuration() : null;

        JsonLinesQueryCallback callback = null;
        final Map<String, String> incomingAttributes = flowFile == null ? Map.of() : flowFile.getAttributes();

        try {
            final List<Object> parameters = getParameters(context, flowFile);
            service.validateStatement(cql, type, parameters.size());

            if (type == CqlStatementType.SELECT) {
                final int fetchSize = context.getProperty(FETCH_SIZE).evaluateAttributeExpressions(flowFile).asInteger();
                final long rowsPerFlowFile = context.getProperty(ROWS_PER_FLOW_FILE).evaluateAttributeExpressions().asLong();
                final int minQueued = context.getProperty(MIN_FLOW_FILES_QUEUED).evaluateAttributeExpressions().asInteger();

                callback = new JsonLinesQueryCallback(flowFile, session, getLogger(), rowsPerFlowFile, minQueued);
                service.query(cql, parameters, callback, new QueryOverrides(fetchSize, timeout));

                if (callback.isEmpty() && flowFile != null) {
                    session.transfer(flowFile, REL_ORIGINAL);
                }
            } else {
                final CqlStatementResult result = service.execute(cql, parameters, new QueryOverrides(null, timeout));
                flowFile = session.putAllAttributes(flowFile, Map.of(
                        ATTRIBUTE_STATEMENT_TYPE, type.name(),
                        ATTRIBUTE_APPLIED, String.valueOf(result.wasApplied())));
                session.transfer(flowFile, REL_SUCCESS);
            }
        } catch (final QueryFailureException e) {
            getLogger().error("Cluster error running CQL {} for {}, routing to retry", type, flowFile, e);
            routeFailure(context, session, flowFile, incomingAttributes, callback, REL_RETRY);
        } catch (final RuntimeException e) {
            getLogger().error("Unable to run CQL {} for {}", type, flowFile, e);
            routeFailure(context, session, flowFile, incomingAttributes, callback, REL_FAILURE);
        }
    }

    /**
     * Routes the incoming FlowFile to {@code relationship}. If a SELECT already released the FlowFile, it is no
     * longer the session's to route, so a replacement carrying its attributes (and, for a retry, how many rows
     * were already sent) takes its place. With no incoming connection there is nothing to route, so yield.
     */
    private void routeFailure(final ProcessContext context, final ProcessSession session, final FlowFile flowFile,
                              final Map<String, String> incomingAttributes, final JsonLinesQueryCallback callback,
                              final Relationship relationship) {
        if (!context.hasIncomingConnection()) {
            context.yield();
            return;
        }

        FlowFile toRoute = flowFile;
        if (toRoute == null || (callback != null && callback.hasSentOriginal())) {
            toRoute = session.putAllAttributes(session.create(), incomingAttributes);
        }

        if (relationship == REL_RETRY && callback != null) {
            toRoute = session.putAttribute(toRoute, ATTRIBUTE_ROWS_SENT, String.valueOf(callback.rowsReleased()));
        }

        session.transfer(relationship == REL_RETRY ? session.penalize(toRoute) : toRoute, relationship);
    }

    /** Collects the cql.arg.<position> properties in bind-marker order, evaluated against {@code flowFile}. */
    private List<Object> getParameters(final ProcessContext context, final FlowFile flowFile) {
        final SortedMap<Integer, Object> byPosition = new TreeMap<>();

        for (final PropertyDescriptor descriptor : context.getProperties().keySet()) {
            if (descriptor.isDynamic()) {
                final Matcher matcher = PARAMETER_PATTERN.matcher(descriptor.getName());
                if (matcher.matches()) {
                    byPosition.put(Integer.valueOf(matcher.group("position")),
                            context.getProperty(descriptor).evaluateAttributeExpressions(flowFile).getValue());
                }
            }
        }

        return new ArrayList<>(byPosition.values());
    }
}
