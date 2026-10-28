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

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatementBuilder;
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.servererrors.QueryExecutionException;
import com.datastax.oss.driver.api.core.servererrors.SyntaxError;
import com.datastax.oss.driver.api.core.ssl.ProgrammaticSslEngineFactory;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.core.type.codec.registry.MutableCodecRegistry;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.delete.DeleteSelection;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.insert.InsertInto;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import com.datastax.oss.driver.api.querybuilder.update.UpdateStart;
import com.datastax.oss.driver.api.querybuilder.update.UpdateWithAssignments;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.VerifiableControllerService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import io.github.mikethomsen.nifi.cql.cassandra.mapping.CassandraUdtSchemaMapper;
import io.github.mikethomsen.nifi.cql.cassandra.mapping.CharacterCodec;
import io.github.mikethomsen.nifi.cql.cassandra.mapping.ExactNumbers;
import io.github.mikethomsen.nifi.cql.cassandra.mapping.FlexibleBigIntCodec;
import io.github.mikethomsen.nifi.cql.cassandra.mapping.FlexibleBooleanCodec;
import io.github.mikethomsen.nifi.cql.cassandra.mapping.FlexibleCounterCodec;
import io.github.mikethomsen.nifi.cql.cassandra.mapping.FlexibleDateCodec;
import io.github.mikethomsen.nifi.cql.cassandra.mapping.FlexibleDoubleCodec;
import io.github.mikethomsen.nifi.cql.cassandra.mapping.FlexibleFloatCodec;
import io.github.mikethomsen.nifi.cql.cassandra.mapping.FlexibleIntCodec;
import io.github.mikethomsen.nifi.cql.cassandra.mapping.FlexibleSmallIntCodec;
import io.github.mikethomsen.nifi.cql.cassandra.mapping.FlexibleTimeCodec;
import io.github.mikethomsen.nifi.cql.cassandra.mapping.FlexibleTinyIntCodec;
import io.github.mikethomsen.nifi.cql.cassandra.mapping.JavaSQLDateCodec;
import io.github.mikethomsen.nifi.cql.cassandra.mapping.JavaSQLTimeCodec;
import io.github.mikethomsen.nifi.cql.cassandra.mapping.JavaSQLTimestampCodec;
import io.github.mikethomsen.nifi.cql.cassandra.mapping.RejectedValueException;
import io.github.mikethomsen.nifi.cql.api.constants.CqlBatchType;
import io.github.mikethomsen.nifi.cql.api.constants.CqlStatementType;
import io.github.mikethomsen.nifi.cql.api.constants.PrimaryKeyFieldType;
import io.github.mikethomsen.nifi.cql.api.constants.UpdateMethod;
import io.github.mikethomsen.nifi.cql.api.exception.QueryFailureException;
import io.github.mikethomsen.nifi.cql.api.lookup.CqlCell;
import io.github.mikethomsen.nifi.cql.api.lookup.CqlRow;
import io.github.mikethomsen.nifi.cql.api.lookup.CqlStatementResult;
import io.github.mikethomsen.nifi.cql.api.lookup.impl.StandardCqlCell;
import io.github.mikethomsen.nifi.cql.api.lookup.impl.StandardCqlRow;
import io.github.mikethomsen.nifi.cql.api.lookup.impl.StandardCqlStatementResult;
import io.github.mikethomsen.nifi.cql.api.metadata.PrimaryKey;
import io.github.mikethomsen.nifi.cql.api.metadata.PrimaryKeyIdentifier;
import io.github.mikethomsen.nifi.cql.api.metadata.PrimaryKeyMetadata;
import io.github.mikethomsen.nifi.cql.api.metadata.QualifiedTableName;
import io.github.mikethomsen.nifi.cql.api.service.AbstractCQLExecutionService;
import io.github.mikethomsen.nifi.cql.api.service.CQLExecutionService;
import io.github.mikethomsen.nifi.cql.api.service.CQLQueryCallback;
import io.github.mikethomsen.nifi.cql.api.service.ContactPoints;
import io.github.mikethomsen.nifi.cql.api.service.CqlStatements;
import io.github.mikethomsen.nifi.cql.api.service.QueryOverrides;
import io.github.mikethomsen.nifi.cql.api.service.TimeToLive;
import io.github.mikethomsen.nifi.cql.api.service.WriteOverrides;
import org.apache.nifi.ssl.SSLContextProvider;

import java.io.File;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.net.ssl.SSLContext;

import static io.github.mikethomsen.nifi.cql.cassandra.mapping.RejectedValueException.shown;

@Tags({"cassandra", "cql", "database", "connection", "session", "pooling"})
@CapabilityDescription("Provides a CQL connection session for CQL processors and controller services to work with Apache Cassandra. "
        + "Use this service for Apache Cassandra clusters; use ScyllaDBCQLExecutionService for ScyllaDB.")
@SeeAlso(classNames = {
        "io.github.mikethomsen.nifi.cql.processors.PutCQLRecord",
        "io.github.mikethomsen.nifi.cql.processors.ExecuteCQLQueryRecord",
        "io.github.mikethomsen.nifi.cql.cache.CQLDistributedMapCache",
        "io.github.mikethomsen.nifi.cql.scylladb.ScyllaDBCQLExecutionService"
})
public class CassandraCQLExecutionService extends AbstractCQLExecutionService implements CQLExecutionService, VerifiableControllerService {

    /**
     * The column a conditional statement's outcome arrives in. Named with brackets by the protocol precisely
     * because no CQL identifier may contain them, so it can never collide with a real column.
     */
    private static final String APPLIED_COLUMN = "[applied]";

    /**
     * Bind marker name for an explicit per-record write timestamp (see {@link WriteOverrides#timestampField()}).
     * Namespaced to make an accidental collision with a real column name effectively impossible.
     */
    private static final String WRITE_TIMESTAMP_BIND_MARKER = "nifi_write_timestamp";

    private static final boolean VALUE_LOGGING_ENABLED = RejectedValueException.VALUE_LOGGING_ENABLED;

    private CqlSession cassandraSession;

    private int pageSize;

    private Duration defaultTtl;

    public static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            CONTACT_POINTS,
            DATACENTER,
            KEYSPACE,
            USERNAME,
            PASSWORD,
            PROP_SSL_CONTEXT_SERVICE,
            HOSTNAME_VALIDATION,
            FETCH_SIZE,
            READ_TIMEOUT,
            CONNECT_TIMEOUT,
            CONSISTENCY_LEVEL,
            COMPRESSION_TYPE,
            DEFAULT_TTL,
            DRIVER_CONFIGURATION_FILE
    );

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    /**
     * Rejects a username without a password, or the reverse. Credentials are only sent when both are present,
     * so half a pair would otherwise connect with none at all and look as if it had worked.
     */
    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(context));

        if (context.getProperty(USERNAME).isSet() != context.getProperty(PASSWORD).isSet()) {
            results.add(new ValidationResult.Builder()
                    .subject("Credentials")
                    .valid(false)
                    .explanation("Username and Password must be set together, or both left unset")
                    .build());
        }

        return results;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        connectToCassandra(context);
    }

    /** Supplies the session directly, so a test can stand in for the cluster without connecting to one. */
    void setSession(final CqlSession session) {
        this.cassandraSession = session;
    }

    @OnDisabled
    public void onDisabled() {
        if (cassandraSession != null) {
            cassandraSession.close();
            cassandraSession = null;
        }
    }

    private String defaultKeyspace;

    /**
     * Returns the given table name with the connection's default keyspace filled in when it is unqualified, so
     * metadata lookups have the explicit keyspace they require.
     */
    private QualifiedTableName resolveKeyspace(QualifiedTableName table) {
        return table.isQualified() ? table : new QualifiedTableName(defaultKeyspace, table.table());
    }

    private void connectToCassandra(ConfigurationContext context) {
        if (cassandraSession == null) {
            this.pageSize = context.getProperty(FETCH_SIZE).evaluateAttributeExpressions().asInteger();

            final PropertyValue defaultTtlProperty = context.getProperty(DEFAULT_TTL).evaluateAttributeExpressions();
            this.defaultTtl = defaultTtlProperty.isSet() ? defaultTtlProperty.asDuration() : null;
            // Validation cannot check a value from Expression Language, so it is checked here, once evaluated:
            // a TTL out of range fails enabling rather than every write.
            if (defaultTtl != null) {
                TimeToLive.toSeconds(defaultTtl);
            }

            // Recorded here rather than in buildSession, which verify() also calls: verifying a different
            // keyspace must not change the one this enabled service resolves table names against.
            this.defaultKeyspace = context.getProperty(KEYSPACE).evaluateAttributeExpressions().getValue();

            final CqlSession cqlSession = buildSession(context);

            try {
                MutableCodecRegistry codecRegistry =
                        (MutableCodecRegistry) cqlSession.getContext().getCodecRegistry();

                codecRegistry.register(new JavaSQLTimestampCodec());
                codecRegistry.register(new JavaSQLDateCodec());
                codecRegistry.register(new JavaSQLTimeCodec());
                codecRegistry.register(new CharacterCodec());
                codecRegistry.register(new FlexibleCounterCodec());
                codecRegistry.register(new FlexibleBooleanCodec());
                codecRegistry.register(new FlexibleTinyIntCodec());
                codecRegistry.register(new FlexibleSmallIntCodec());
                codecRegistry.register(new FlexibleIntCodec());
                codecRegistry.register(new FlexibleBigIntCodec());
                codecRegistry.register(new FlexibleFloatCodec());
                codecRegistry.register(new FlexibleDoubleCodec());
                codecRegistry.register(new FlexibleDateCodec());
                codecRegistry.register(new FlexibleTimeCodec());
            } catch (Exception ex) {
                cqlSession.close();
                throw new ProcessException("There was an error registering conversion codecs.", ex);
            }

            cassandraSession = cqlSession;
        }
    }

    private CqlSession buildSession(final ConfigurationContext context) {
        return buildSession(context, true);
    }

    /**
     * @param bindKeyspace when {@code false}, the session is opened without binding the configured keyspace.
     * {@link #verify(ConfigurationContext, ComponentLog, Map)} passes {@code false} so a keyspace that does
     * not exist is reported by the dedicated "Verify Keyspace" step rather than failing session creation
     * outright - the driver rejects an unknown keyspace during {@code CqlSession.build()}.
     */
    private CqlSession buildSession(final ConfigurationContext context, final boolean bindKeyspace) {
        final String consistencyLevel = context.getProperty(CONSISTENCY_LEVEL).getValue();
        final String compression = context.getProperty(COMPRESSION_TYPE).getValue();
        final String contactPointList = context.getProperty(CONTACT_POINTS).evaluateAttributeExpressions().getValue();

        List<InetSocketAddress> contactPoints = getContactPoints(contactPointList);

        // Set up the client for secure (SSL/TLS communications) if configured to do so
        final SSLContextProvider sslContextProvider =
                context.getProperty(PROP_SSL_CONTEXT_SERVICE).asControllerService(SSLContextProvider.class);
        final SslEngineFactory sslEngineFactory;

        if (sslContextProvider == null) {
            sslEngineFactory = null;
        } else {
            sslEngineFactory = sslEngineFactory(sslContextProvider.createContext(), context.getProperty(HOSTNAME_VALIDATION).asBoolean());
        }

        final String username;
        final String password;
        PropertyValue usernameProperty = context.getProperty(USERNAME).evaluateAttributeExpressions();
        PropertyValue passwordProperty = context.getProperty(PASSWORD).evaluateAttributeExpressions();

        if (usernameProperty != null && passwordProperty != null) {
            username = usernameProperty.getValue();
            password = passwordProperty.getValue();
        } else {
            username = null;
            password = null;
        }

        final Duration readTimeout = context.getProperty(READ_TIMEOUT).evaluateAttributeExpressions().asDuration();
        final Duration connectTimeout = context.getProperty(CONNECT_TIMEOUT).evaluateAttributeExpressions().asDuration();

        final String datacenter = context.getProperty(DATACENTER).evaluateAttributeExpressions().getValue();

        final String sessionKeyspace = context.getProperty(KEYSPACE).evaluateAttributeExpressions().getValue();

        final DriverConfigLoader propertyBasedLoader =
                DriverConfigLoader.programmaticBuilder()
                        .withDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT, connectTimeout)
                        .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, readTimeout)
                        .withString(DefaultDriverOption.REQUEST_CONSISTENCY, consistencyLevel)
                        .withString(DefaultDriverOption.PROTOCOL_COMPRESSION, compression)
                        .build();

        final DriverConfigLoader loader = buildConfigLoader(context, propertyBasedLoader);

        CqlSessionBuilder builder = CqlSession.builder()
                .addContactPoints(contactPoints);

        // Validation requires the two together, but either may still come out blank once Expression Language
        // is evaluated. Connecting anonymously in that case would hide the misconfiguration.
        if (StringUtils.isBlank(username) != StringUtils.isBlank(password)) {
            throw new IllegalArgumentException("Username and Password must both have a value, or both be unset");
        }

        if (StringUtils.isNotBlank(username)) {
            builder = builder.withAuthCredentials(username, password);
        }

        builder = builder
                .withSslEngineFactory(sslEngineFactory)
                .withLocalDatacenter(datacenter)
                .withConfigLoader(loader);

        if (bindKeyspace) {
            builder = builder.withKeyspace(sessionKeyspace);
        }

        return builder.build();
    }

    /**
     * Built explicitly rather than through {@code CqlSessionBuilder#withSslContext}, which turns hostname
     * validation off.
     */
    static SslEngineFactory sslEngineFactory(final SSLContext sslContext, final boolean hostnameValidation) {
        return new ProgrammaticSslEngineFactory(sslContext, null, hostnameValidation);
    }

    /**
     * Builds the {@link DriverConfigLoader} used to open the session. When a Driver Configuration File is
     * configured, it is composed as the primary source ahead of {@code propertyBasedLoader}, so any option
     * defined in the file overrides the corresponding connection property, while options absent from the
     * file still fall back to the property-derived configuration. The file uses the driver's standard
     * typesafe-config format, which the ScyllaDB Java Driver reads identically, so this method requires no
     * override for ScyllaDB compatibility.
     */
    protected DriverConfigLoader buildConfigLoader(final ConfigurationContext context, final DriverConfigLoader propertyBasedLoader) {
        final PropertyValue configFileProperty = context.getProperty(DRIVER_CONFIGURATION_FILE);

        if (configFileProperty == null || !configFileProperty.isSet()) {
            return propertyBasedLoader;
        }

        final String configFilePath = configFileProperty.evaluateAttributeExpressions().getValue();
        final DriverConfigLoader fileBasedLoader = DriverConfigLoader.fromFile(new File(configFilePath));

        return DriverConfigLoader.compose(fileBasedLoader, propertyBasedLoader);
    }

    @Override
    public List<ConfigVerificationResult> verify(final ConfigurationContext context, final ComponentLog verificationLogger, final Map<String, String> variables) {
        final List<ConfigVerificationResult> results = new ArrayList<>();

        final String datacenter = context.getProperty(DATACENTER).evaluateAttributeExpressions().getValue();

        CqlSession session = null;
        try {
            // Opened without binding the keyspace: the driver rejects an unknown keyspace during build(),
            // which would collapse a missing-keyspace failure into this step instead of the "Verify Keyspace"
            // one below. Keyspace existence is checked explicitly against the session metadata further down.
            session = buildSession(context, false);
            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("Establish Connection")
                    .outcome(ConfigVerificationResult.Outcome.SUCCESSFUL)
                    .explanation("Successfully connected to Cassandra using datacenter [" + datacenter + "]")
                    .build());
        } catch (final Exception e) {
            verificationLogger.warn("Failed to establish Cassandra connection using datacenter [{}]: {}", datacenter, e.getMessage());
            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("Establish Connection")
                    .outcome(ConfigVerificationResult.Outcome.FAILED)
                    .explanation("Failed to connect using datacenter [" + datacenter + "]: " + e.getMessage())
                    .build());
        }

        if (session != null) {
            try {
                // CqlSessionBuilder.build() succeeds even when no node belongs to the configured local
                // datacenter: the driver only filters nodes by datacenter when computing a query plan for
                // an actual request. Executing a trivial statement here forces that node selection so a
                // bad datacenter name is caught during verification instead of on first real query.
                try {
                    session.execute("SELECT release_version FROM system.local");
                    results.add(new ConfigVerificationResult.Builder()
                            .verificationStepName("Verify Datacenter")
                            .outcome(ConfigVerificationResult.Outcome.SUCCESSFUL)
                            .explanation("Found an available node in datacenter [" + datacenter + "]")
                            .build());
                } catch (final Exception e) {
                    verificationLogger.warn("No available node found in datacenter [{}]: {}", datacenter, e.getMessage());
                    results.add(new ConfigVerificationResult.Builder()
                            .verificationStepName("Verify Datacenter")
                            .outcome(ConfigVerificationResult.Outcome.FAILED)
                            .explanation("No available node found in datacenter [" + datacenter + "]: " + e.getMessage())
                            .build());
                }

                final String keyspaceName = context.getProperty(KEYSPACE).isSet()
                        ? context.getProperty(KEYSPACE).evaluateAttributeExpressions().getValue() : null;

                if (StringUtils.isNotBlank(keyspaceName)) {
                    final boolean keyspaceFound = session.getMetadata().getKeyspace(keyspaceName).isPresent();
                    results.add(new ConfigVerificationResult.Builder()
                            .verificationStepName("Verify Keyspace")
                            .outcome(keyspaceFound ? ConfigVerificationResult.Outcome.SUCCESSFUL : ConfigVerificationResult.Outcome.FAILED)
                            .explanation(keyspaceFound
                                    ? "Found keyspace [" + keyspaceName + "]"
                                    : "Keyspace [" + keyspaceName + "] was not found in the cluster")
                            .build());
                }
            } finally {
                session.close();
            }
        }

        return results;
    }

    private List<InetSocketAddress> getContactPoints(String contactPointList) {
        return ContactPoints.parse(contactPointList).stream()
                .map(contactPoint -> new InetSocketAddress(contactPoint.host(), contactPoint.port()))
                .collect(Collectors.toList());
    }

    private PrimaryKey convertTableMetadata(TableMetadata metadata) {
        List<PrimaryKeyMetadata> partitions = new ArrayList<>();
        List<PrimaryKeyMetadata> clustering = new ArrayList<>();

        int loc = 0;
        for (ColumnMetadata m : metadata.getPartitionKey()) {
            partitions.add(new PrimaryKeyMetadata(m.getName().asInternal(), loc++, PrimaryKeyFieldType.PARTITION));
        }

        loc = 0;
        for (ColumnMetadata m : metadata.getClusteringColumns().keySet()) {
            clustering.add(new PrimaryKeyMetadata(m.getName().asInternal(), loc++, PrimaryKeyFieldType.CLUSTERING));
        }

        return new PrimaryKey(partitions, clustering);
    }

    private static String valuesFragment(final Object[] values) {
        return VALUE_LOGGING_ENABLED ? " with values " + Arrays.toString(values) : "";
    }

    private static String valuesFragment(final List<Object> values) {
        return VALUE_LOGGING_ENABLED && values != null && !values.isEmpty() ? " with values " + values : "";
    }

    @Override
    public void query(String cql, List<Object> parameters, CQLQueryCallback callback, QueryOverrides overrides) throws QueryFailureException {
        if (getLogger().isDebugEnabled()) {
            getLogger().debug("Executing CQL query: {}{}", cql, valuesFragment(parameters));
        }

        BoundStatement boundStatement = bindStatement(cql, parameters, overrides);

        AtomicReference<RecordSchema> schemaReference = new AtomicReference<>();

        // The whole lifecycle of the query - the initial execute() as well as every page fetch triggered by
        // hasNext()/next() as the ResultSet is iterated - can surface a QueryExecutionException, so all of it
        // is covered by one try, rather than only the page-fetch calls.
        try {
            ResultSet results = cassandraSession.execute(boundStatement);
            Iterator<Row> resultsIterator = results.iterator();
            long rowNumber = 0;

            List<String> fieldNames = null;
            List<org.apache.nifi.serialization.record.DataType> convertedTypes = null;

            while (resultsIterator.hasNext()) {
                Row row = resultsIterator.next();

                if (schemaReference.get() == null) {
                    fieldNames = fieldNames(results.getColumnDefinitions());
                    Schema generatedAvroSchema = createSchema(results, fieldNames);
                    RecordSchema converted = AvroTypeUtil.createSchema(generatedAvroSchema);
                    schemaReference.set(converted);
                    convertedTypes = recordTypesNeedingConversion(results.getColumnDefinitions(), fieldNames, converted);
                }

                Map<String, Object> resultMap = new HashMap<>();

                for (int x = 0; x < fieldNames.size(); x++) {
                    final org.apache.nifi.serialization.record.DataType type = convertedTypes.get(x);
                    resultMap.put(fieldNames.get(x), type == null ? row.getObject(x) : toRecordValue(row.getObject(x), type));
                }

                MapRecord record = new MapRecord(schemaReference.get(), resultMap);

                callback.receive(++rowNumber, record, resultsIterator.hasNext());
            }
        // Each of these is worth another attempt: a coordinator/replica-level execution failure, no reachable
        // node, or a client-side request timeout. Deliberately not their DriverException base type - the
        // validation errors under it (SyntaxError, InvalidQueryException, UnauthorizedException) can never
        // succeed on a retry, so they belong on the failure path below.
        } catch (QueryExecutionException | AllNodesFailedException | DriverTimeoutException qee) {
            getLogger().error("Error executing query", qee);
            callback.clear();
            throw new QueryFailureException();
        } catch (Exception ex) {
            callback.clear();
            throw new ProcessException("Error querying CQL", ex);
        }
    }

    @Override
    public void validateStatement(final String cql, final CqlStatementType expectedType, final int parameterCount)
            throws QueryFailureException {
        final CqlStatementType actualType = CqlStatements.typeOf(cql);
        if (actualType != expectedType) {
            throw new IllegalArgumentException("The statement is " + (actualType == null ? "not a SELECT, INSERT, UPDATE or DELETE" : "a " + actualType)
                    + " but " + expectedType + " was expected");
        }

        if ((expectedType == CqlStatementType.UPDATE || expectedType == CqlStatementType.DELETE) && !CqlStatements.hasWhereClause(cql)) {
            throw new IllegalArgumentException("A " + expectedType + " without a WHERE clause is not allowed");
        }

        final PreparedStatement preparedStatement;
        try {
            preparedStatement = cassandraSession.prepare(cql);
        } catch (final SyntaxError | InvalidQueryException e) {
            // The cluster has understood the request and rejected the statement - unparseable, or naming a
            // keyspace, table or column that does not exist - which a retry cannot fix.
            throw new IllegalArgumentException("The cluster rejected the statement: " + e.getMessage(), e);
        } catch (final QueryExecutionException | AllNodesFailedException | DriverTimeoutException e) {
            getLogger().error("Error preparing statement for validation", e);
            throw new QueryFailureException();
        }

        final int markerCount = preparedStatement.getVariableDefinitions().size();
        if (markerCount != parameterCount) {
            throw new IllegalArgumentException("The statement has " + markerCount + " bind marker(s) but " + parameterCount
                    + " parameter(s) were supplied");
        }
    }

    @Override
    public CqlStatementResult execute(final String cql, final List<Object> parameters, final QueryOverrides overrides)
            throws QueryFailureException {
        if (getLogger().isDebugEnabled()) {
            getLogger().debug("Executing CQL statement: {}{}", cql, valuesFragment(parameters));
        }

        final BoundStatement boundStatement = bindStatement(cql, parameters, overrides);

        try {
            final ResultSet results = cassandraSession.execute(boundStatement);
            final boolean wasApplied = results.wasApplied();

            // The outcome column is present exactly when the statement was conditional - wasApplied() alone
            // cannot tell us, since it also returns true for every unconditional statement.
            final boolean conditional = results.getColumnDefinitions().contains(APPLIED_COLUMN);

            // A conditional write that applied has no prior value to report, so it reports no rows. The
            // backends disagree about what they send in that case: Cassandra returns the outcome column
            // alone, ScyllaDB returns the other columns alongside it. Normalizing here is the whole point of
            // having one API over both - otherwise callers would find rows().isEmpty() backend-dependent.
            if (conditional && wasApplied) {
                return new StandardCqlStatementResult(true, List.of());
            }

            final List<CqlRow> rows = new ArrayList<>();
            for (final Row row : results) {
                rows.add(new StandardCqlRow(toCells(row)));
            }

            return new StandardCqlStatementResult(wasApplied, rows);
        } catch (QueryExecutionException | AllNodesFailedException | DriverTimeoutException qee) {
            getLogger().error("Error executing statement", qee);
            throw new QueryFailureException();
        } catch (Exception ex) {
            throw new ProcessException("Error executing CQL", ex);
        }
    }

    /**
     * Converts one driver row into cells, dropping the {@code [applied]} conditional-outcome column and marking
     * any column whose CQL type has no JDK representation as unsupported.
     */
    private List<CqlCell> toCells(final Row row) {
        final ColumnDefinitions definitions = row.getColumnDefinitions();
        final List<CqlCell> cells = new ArrayList<>(definitions.size());

        for (int index = 0; index < definitions.size(); index++) {
            final String columnName = definitions.get(index).getName().toString();
            if (APPLIED_COLUMN.equals(columnName)) {
                continue;
            }

            final DataType columnType = definitions.get(index).getType();
            cells.add(hasJdkRepresentation(columnType)
                    ? new StandardCqlCell(columnName, row.getObject(index))
                    : StandardCqlCell.unsupported(columnName, columnType.asCql(true, false)));
        }

        return cells;
    }

    /**
     * Reports whether a CQL type decodes to a JDK type rather than a driver class, recursing through collection
     * types so a {@code list<some_udt>} is rejected for its element type.
     */
    private static boolean hasJdkRepresentation(final DataType type) {
        return switch (type) {
            case ListType list -> hasJdkRepresentation(list.getElementType());
            case SetType set -> hasJdkRepresentation(set.getElementType());
            case MapType map -> hasJdkRepresentation(map.getKeyType()) && hasJdkRepresentation(map.getValueType());
            default -> !(type instanceof UserDefinedType || type instanceof TupleType || DataTypes.DURATION.equals(type));
        };
    }

    /**
     * Builds and binds a statement, applying any per-call fetch size and timeout overrides. Shared by
     * {@link #query} and {@link #execute}, which differ only in what they do with the result.
     */
    private BoundStatement bindStatement(final String cql, final List<Object> parameters, final QueryOverrides overrides) {
        SimpleStatementBuilder statementBuilder = SimpleStatement.builder(cql)
                .setPageSize(resolveFetchSize(overrides, pageSize));

        final Duration timeoutOverride = resolveTimeoutOverride(overrides);
        if (timeoutOverride != null) {
            statementBuilder = statementBuilder.setTimeout(timeoutOverride);
        }

        final SimpleStatement statement = statementBuilder.build();
        final PreparedStatement preparedStatement = prepare(statement);

        return parameters != null && !parameters.isEmpty()
                ? encode(preparedStatement, toBindValues(preparedStatement, parameters))
                : preparedStatement.bind();
    }

    /**
     * Prepares a statement, reporting a cluster-level failure the same way execution does. A node can drop
     * while a statement is being prepared just as it can while one is running, and a retry is as likely to
     * fix it. A statement the cluster rejects as invalid is left to propagate, since a retry cannot fix that.
     */
    private PreparedStatement prepare(final SimpleStatement statement) throws QueryFailureException {
        try {
            return cassandraSession.prepare(statement);
        } catch (QueryExecutionException | AllNodesFailedException | DriverTimeoutException e) {
            getLogger().error("Error preparing statement", e);
            throw new QueryFailureException();
        }
    }

    static int resolveFetchSize(final QueryOverrides overrides, final int defaultFetchSize) {
        return overrides != null && overrides.fetchSize() != null ? overrides.fetchSize() : defaultFetchSize;
    }

    static Duration resolveTimeoutOverride(final QueryOverrides overrides) {
        return overrides != null ? overrides.timeout() : null;
    }

    static BatchType toDriverBatchType(final CqlBatchType batchType) {
        return switch (batchType) {
            case LOGGED -> BatchType.LOGGED;
            case UNLOGGED -> BatchType.UNLOGGED;
            case COUNTER -> BatchType.COUNTER;
        };
    }

    static Integer resolveTtlSeconds(final WriteOverrides overrides, final Duration defaultTtl) {
        final Duration ttl = overrides != null && overrides.ttl() != null ? overrides.ttl() : defaultTtl;
        return ttl != null ? TimeToLive.toSeconds(ttl) : null;
    }

    static boolean hasTimestampOverride(final WriteOverrides overrides) {
        return overrides != null && overrides.timestampField() != null;
    }

    /**
     * Converts a record field's value into epoch microseconds, the unit Cassandra/ScyllaDB expect for an
     * explicit write timestamp (distinct from the millisecond precision of the CQL {@code timestamp} type).
     */
    static long toEpochMicros(final Object value) {
        final Instant instant;
        if (value instanceof Instant instantValue) {
            instant = instantValue;
        } else if (value instanceof java.util.Date dateValue) {
            instant = dateValue.toInstant();
        } else if (value instanceof Number numberValue) {
            instant = Instant.ofEpochMilli(ExactNumbers.toEpochMillis(numberValue));
        } else {
            throw new IllegalArgumentException("Cannot derive a write timestamp from a value of type "
                    + (value == null ? "null" : value.getClass().getName()));
        }

        // Microseconds since the epoch overflow a long about 292,000 years out, well inside what an Instant holds.
        try {
            return Math.addExact(Math.multiplyExact(instant.getEpochSecond(), 1_000_000L), instant.getNano() / 1_000L);
        } catch (final ArithmeticException e) {
            throw new RejectedValueException("A write timestamp cannot be derived: the value is out of range for a CQL write timestamp", null);
        }
    }

    protected GeneratedResult generateInsert(QualifiedTableName table, Record sample,
                                              Map<PrimaryKeyIdentifier, RecordPath> primaryKeyOverrides,
                                              Integer ttlSeconds, String timestampField) {
        InsertInto insertQuery = table.isQualified()
                ? QueryBuilder.insertInto(table.keyspace(), table.table())
                : QueryBuilder.insertInto(table.table());
        List<String> keys = new ArrayList<>();

        RegularInsert regularInsert = null;
        final String timestampOnlyField = timestampOnlyField(table, timestampField);
        for (String fieldName : columnNamesIncludingOverrideOnly(table, sample, primaryKeyOverrides)) {
            if (fieldName.equals(timestampOnlyField)) {
                continue;
            }
            if (regularInsert == null) {
                regularInsert = insertQuery.value(fieldName, QueryBuilder.bindMarker(fieldName));
            } else {
                regularInsert = regularInsert.value(fieldName, QueryBuilder.bindMarker(fieldName));
            }

            keys.add(fieldName);
        }

        if (regularInsert == null) {
            throw new ProcessException("Could not build an insert statement from the supplied record");
        }

        Insert insert = regularInsert;
        if (ttlSeconds != null) {
            insert = insert.usingTtl(ttlSeconds);
        }
        if (timestampField != null) {
            // A separate named bind marker (set by name on the bound statement, not part of "keys") rather than
            // a literal, since - unlike TTL - each record in a batch can carry a different write timestamp.
            insert = insert.usingTimestamp(QueryBuilder.bindMarker(WRITE_TIMESTAMP_BIND_MARKER));
        }

        return new GeneratedResult(insert.build(), keys);
    }

    /**
     * Returns every schema field name in schema order, followed by any primary key override target column for
     * this table that has no same-named schema field.
     */
    private List<String> columnNamesIncludingOverrideOnly(QualifiedTableName table, Record sample,
                                                            Map<PrimaryKeyIdentifier, RecordPath> primaryKeyOverrides) {
        final List<String> columnNames = new ArrayList<>(fieldsToWrite(table, sample, primaryKeyOverrides));
        for (String overrideFieldName : overrideFieldNamesForTable(table, primaryKeyOverrides)) {
            if (columnNames.stream().noneMatch(name -> sameCqlName(name, overrideFieldName))) {
                columnNames.add(overrideFieldName);
            }
        }
        return columnNames;
    }

    /**
     * The sample record's field names, less any that only supply a primary key override's value: a top-level
     * field that an override for this table reads from, such as {@code customer} for {@code /customer/id}, and
     * that the table has no column for. Writing one would fail, since there is nowhere to write it. Any other
     * field is kept, so a field that matches no column still fails the write, as before. The source fields are
     * found by evaluating each override against the sample, since a compiled RecordPath does not expose its
     * segments.
     */
    private List<String> fieldsToWrite(QualifiedTableName table, Record sample,
                                       Map<PrimaryKeyIdentifier, RecordPath> primaryKeyOverrides) {
        final List<String> fieldNames = sample.getSchema().getFieldNames();
        final Set<String> overrideSources = new HashSet<>();
        for (Map.Entry<PrimaryKeyIdentifier, RecordPath> override : overridesForTable(table, primaryKeyOverrides)) {
            override.getValue().evaluate(sample).getSelectedFields()
                    .forEach(value -> overrideSources.add(topLevelFieldName(value)));
        }
        if (fieldNames.stream().noneMatch(overrideSources::contains)) {
            return fieldNames;
        }

        final QualifiedTableName resolved = resolveKeyspace(table);
        final Optional<TableMetadata> metadata = resolved.isQualified()
                ? cassandraSession.getMetadata().getKeyspace(resolved.keyspace()).flatMap(keyspace -> keyspace.getTable(resolved.table()))
                : Optional.empty();
        if (metadata.isEmpty()) {
            return fieldNames;
        }

        return fieldNames.stream()
                .filter(fieldName -> !overrideSources.contains(fieldName) || hasColumn(metadata.get(), fieldName))
                .toList();
    }

    /**
     * Whether the table has a column for this record field. The name is read as CQL, as an unquoted field name is
     * everywhere else; one that is not a plain identifier, such as the reserved word {@code order}, cannot be read
     * that way, so it is matched exactly instead.
     */
    private static boolean hasColumn(final TableMetadata table, final String fieldName) {
        try {
            return table.getColumn(fieldName).isPresent();
        } catch (final IllegalArgumentException e) {
            return table.getColumn(CqlIdentifier.fromInternal(fieldName)).isPresent();
        }
    }

    /**
     * Returns {@code timestampField} when the table has no column of that name, meaning the field only supplies
     * the write timestamp and must not be written as a column; otherwise returns null. A table whose metadata
     * cannot be found keeps the field, so the cluster reports the problem as before.
     */
    private String timestampOnlyField(QualifiedTableName table, String timestampField) {
        if (timestampField == null) {
            return null;
        }

        final QualifiedTableName resolved = resolveKeyspace(table);
        if (!resolved.isQualified()) {
            return null;
        }

        final boolean tableLacksColumn = cassandraSession.getMetadata().getKeyspace(resolved.keyspace())
                .flatMap(keyspace -> keyspace.getTable(resolved.table()))
                .map(tableMetadata -> tableMetadata.getColumn(timestampField).isEmpty())
                .orElse(false);
        return tableLacksColumn ? timestampField : null;
    }

    /**
     * The field names of every primary key override configured for this specific table, resolving an
     * unqualified {@code table} against {@link #defaultKeyspace} the same way {@link #getRecordPathOverride}
     * does.
     */
    private Set<String> overrideFieldNamesForTable(QualifiedTableName table, Map<PrimaryKeyIdentifier, RecordPath> primaryKeyOverrides) {
        return overridesForTable(table, primaryKeyOverrides).stream()
                .map(override -> override.getKey().fieldName())
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /** The name of the record's top-level field that holds {@code value}: the ancestor just below the record itself. */
    private static String topLevelFieldName(final FieldValue value) {
        FieldValue current = value;
        while (current.getParent().flatMap(FieldValue::getParent).isPresent()) {
            current = current.getParent().get();
        }
        return current.getField().getFieldName();
    }

    /** The overrides naming this table, with keyspace and table compared as CQL identifiers, as {@link #getRecordPathOverride} does. */
    private List<Map.Entry<PrimaryKeyIdentifier, RecordPath>> overridesForTable(QualifiedTableName table,
                                                                                 Map<PrimaryKeyIdentifier, RecordPath> primaryKeyOverrides) {
        final QualifiedTableName resolved = resolveKeyspace(table);
        if (!resolved.isQualified()) {
            return List.of();
        }
        return primaryKeyOverrides.entrySet().stream()
                .filter(override -> sameCqlName(override.getKey().keyspace(), resolved.keyspace())
                        && sameCqlName(override.getKey().tableName(), resolved.table()))
                .toList();
    }

    /** Whether {@code key} is a record field or the column of an override for this table, compared as CQL identifiers. */
    private static boolean isKnownKey(final String key, final RecordSchema schema, final Set<String> overrideFieldNames) {
        return schema.getFieldNames().contains(key) || overrideFieldNames.stream().anyMatch(name -> sameCqlName(name, key));
    }

    @Override
    public void insert(QualifiedTableName table, Record record, Map<PrimaryKeyIdentifier, RecordPath> primaryKeyOverrides,
                        WriteOverrides overrides) throws QueryFailureException {
        final boolean useTimestampOverride = hasTimestampOverride(overrides);
        GeneratedResult result = generateInsert(table, record, primaryKeyOverrides, resolveTtlSeconds(overrides, defaultTtl), useTimestampOverride ? overrides.timestampField() : null);
        PreparedStatement preparedStatement = prepare(result.statement);

        Object[] values = getBindValues(table, primaryKeyOverrides, record, preparedStatement, result.keysUsed);
        if (useTimestampOverride) {
            setTimestampBindValue(values, preparedStatement, record, overrides);
        }

        if (getLogger().isDebugEnabled()) {
            getLogger().debug("Executing CQL insert: {}{}", result.statement.getQuery(), valuesFragment(values));
        }

        BoundStatement boundStatement = bind(preparedStatement, values, overrides);

        try {
            cassandraSession.execute(boundStatement);
        } catch (QueryExecutionException | AllNodesFailedException | DriverTimeoutException qee) {
            getLogger().error("Error executing insert", qee);
            throw new QueryFailureException();
        }
    }

    @Override
    public void insert(QualifiedTableName table, List<Record> records, Map<PrimaryKeyIdentifier, RecordPath> primaryKeyOverrides,
                        CqlBatchType batchType, WriteOverrides overrides) throws QueryFailureException {
        if (records == null || records.isEmpty()) {
            return;
        }

        final boolean useTimestampOverride = hasTimestampOverride(overrides);
        BatchStatementBuilder builder = BatchStatement.builder(toDriverBatchType(batchType));
        GeneratedResult result = generateInsert(table, records.get(0), primaryKeyOverrides, resolveTtlSeconds(overrides, defaultTtl), useTimestampOverride ? overrides.timestampField() : null);
        PreparedStatement preparedStatement = prepare(result.statement);

        if (getLogger().isDebugEnabled()) {
            getLogger().debug("Executing CQL {} batch insert of {} record(s): {}", batchType, records.size(), result.statement.getQuery());
        }

        int recordNumber = 0;
        for (Record record : records) {
            recordNumber++;
            Object[] values = getBindValues(table, primaryKeyOverrides, record, preparedStatement, result.keysUsed);
            if (useTimestampOverride) {
                setTimestampBindValue(values, preparedStatement, record, overrides);
            }

            if (VALUE_LOGGING_ENABLED && getLogger().isDebugEnabled()) {
                getLogger().debug("Batch insert record {}{}", recordNumber, valuesFragment(values));
            }

            builder.addStatement(bind(preparedStatement, values, overrides));
        }

        try {
            cassandraSession.execute(builder.build());
        } catch (QueryExecutionException | AllNodesFailedException | DriverTimeoutException qee) {
            getLogger().error("Error executing batch insert", qee);
            throw new QueryFailureException();
        }
    }

    @Override
    public String getTransitUrl(QualifiedTableName tableName) {
        final String qualifiedName = tableName.isQualified()
                ? tableName.keyspace() + "." + tableName.table()
                : tableName.table();
        return cassandraSession.getMetadata().getClusterName()
                .map(clusterName -> transitScheme() + "://" + clusterName + "." + qualifiedName)
                .orElse(transitScheme() + "://" + qualifiedName);
    }

    /** The scheme of {@link #getTransitUrl}, naming the kind of cluster a write went to. */
    protected String transitScheme() {
        return "cassandra";
    }

    /** An Avro name: a letter or underscore, then letters, digits and underscores. */
    private static final Pattern AVRO_NAME = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    /**
     * The record field name for each column of a result, in selection order.
     *
     * <p>A column's own name is kept when it is already a legal Avro name, which covers every plain column. An
     * unaliased function such as {@code system.min(sent_at)} is not, so each run of other characters becomes an
     * underscore: {@code system_min_sent_at}. A name already taken gets {@code _2}, {@code _3} and so on, so a
     * column selected twice keeps both values rather than failing the schema or overwriting one with the other.
     */
    static List<String> fieldNames(final ColumnDefinitions columnDefinitions) {
        final List<String> names = new ArrayList<>(columnDefinitions.size());
        final Set<String> taken = new HashSet<>();
        for (final ColumnDefinition definition : columnDefinitions) {
            final String base = avroName(definition.getName().toString());
            String name = base;
            for (int suffix = 2; !taken.add(name); suffix++) {
                name = base + "_" + suffix;
            }
            names.add(name);
        }
        return names;
    }

    private static String avroName(final String text) {
        if (AVRO_NAME.matcher(text).matches()) {
            return text;
        }
        final String replaced = text.replaceAll("[^A-Za-z0-9_]+", "_").replaceAll("^_+|_+$", "");
        return replaced.isEmpty() || Character.isDigit(replaced.charAt(0)) ? "_" + replaced : replaced;
    }

    /**
     * The record type of each column whose driver value needs {@link #toRecordValue}, and {@code null} for every
     * other column, whose value the driver already decodes to a type the record accepts as it is.
     */
    private static List<org.apache.nifi.serialization.record.DataType> recordTypesNeedingConversion(
            final ColumnDefinitions columnDefinitions, final List<String> fieldNames, final RecordSchema schema) {
        final List<org.apache.nifi.serialization.record.DataType> types = new ArrayList<>(fieldNames.size());
        for (int i = 0; i < fieldNames.size(); i++) {
            types.add(needsRecordConversion(columnDefinitions.get(i).getType())
                    ? schema.getDataType(fieldNames.get(i)).orElseThrow()
                    : null);
        }
        return types;
    }

    /**
     * Whether the driver decodes a column of this CQL type to something other than what its record type holds:
     * a UDT, tuple, blob, inet or set anywhere in the type, or any map, since a record map's keys are text.
     */
    private static boolean needsRecordConversion(final DataType type) {
        return switch (type) {
            case ListType list -> needsRecordConversion(list.getElementType());
            case SetType set -> true;
            case MapType map -> true;
            default -> type instanceof UserDefinedType || type instanceof TupleType
                    || DataTypes.BLOB.equals(type) || DataTypes.INET.equals(type);
        };
    }

    /**
     * Converts a driver value to the form its record type declares, recursing through collections and nested
     * UDTs. A UDT becomes a record, a set a list, a blob its bytes, an inet its address text such as
     * {@code 127.0.0.1}, and a map's keys text. A tuple, or a UDT with no record type to fill (a map key), becomes
     * its CQL literal text, such as {@code (7,'x')}. Anything else, a duration included, is returned as it is: its
     * declared type is a string, which its own text satisfies.
     */
    static Object toRecordValue(final Object value, final org.apache.nifi.serialization.record.DataType type) {
        return switch (value) {
            case UdtValue udt when type instanceof RecordDataType recordType -> toRecord(udt, recordType.getChildSchema());
            case UdtValue udt -> udt.getFormattedContents();
            case TupleValue tuple -> tuple.getFormattedContents();
            case ByteBuffer buffer -> {
                final byte[] bytes = new byte[buffer.remaining()];
                buffer.duplicate().get(bytes);
                yield bytes;
            }
            case InetAddress address -> address.getHostAddress();
            case Collection<?> elements when type instanceof ArrayDataType arrayType ->
                    elements.stream().map(element -> toRecordValue(element, arrayType.getElementType())).toList();
            case Map<?, ?> map when type instanceof MapDataType mapType -> {
                final Map<String, Object> converted = new LinkedHashMap<>();
                map.forEach((key, entry) -> converted.put(mapKeyText(key), toRecordValue(entry, mapType.getValueType())));
                yield converted;
            }
            case null, default -> value;
        };
    }

    /** A map key as text, written for a blob key the way CQL writes a blob literal, such as {@code 0xcafe}. */
    private static String mapKeyText(final Object key) {
        return key instanceof ByteBuffer buffer
                ? "0x" + HexFormat.of().formatHex((byte[]) toRecordValue(buffer, null))
                : String.valueOf(toRecordValue(key, null));
    }

    private static Record toRecord(final UdtValue udt, final RecordSchema schema) {
        final List<CqlIdentifier> names = udt.getType().getFieldNames();
        final Map<String, Object> values = new HashMap<>();
        for (int i = 0; i < names.size(); i++) {
            final String name = names.get(i).asInternal();
            values.put(name, toRecordValue(udt.getObject(i), schema.getDataType(name).orElse(null)));
        }
        return new MapRecord(schema, values);
    }

    /**
     * Creates an Avro schema from the given result set. The metadata (column definitions, data types, etc.) is used
     * to determine a schema for Avro.
     *
     * @param rs The result set from which an Avro schema will be created
     * @param fieldNames the field name for each column, from {@link #fieldNames}
     * @return An Avro schema corresponding to the given result set's metadata
     */
    static Schema createSchema(final ResultSet rs, final List<String> fieldNames) {
        final ColumnDefinitions columnDefinitions = rs.getColumnDefinitions();
        final int nrOfColumns = (columnDefinitions == null ? 0 : columnDefinitions.size());
        String tableName = "NiFi_Cassandra_Query_Record";
        if (nrOfColumns > 0) {
            String tableNameFromMeta = columnDefinitions.get(0).getTable().toString();
            if (!StringUtils.isBlank(tableNameFromMeta)) {
                tableName = avroName(tableNameFromMeta);
            }
        }

        final SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.record(tableName).namespace("any.data").fields();
        if (columnDefinitions != null) {
            final Map<String, Schema> udtSchemaCache = new HashMap<>();
            for (int i = 0; i < nrOfColumns; i++) {
                DataType dataType = columnDefinitions.get(i).getType();
                if (dataType == null) {
                    throw new IllegalArgumentException("No data type for column[" + i + "] with name "
                            + columnDefinitions.get(i).getName());
                }

                builder.name(fieldNames.get(i))
                        .type(CassandraUdtSchemaMapper.toAvroSchema(dataType, udtSchemaCache))
                        .noDefault();
            }
        }
        return builder.endRecord();
    }

    protected GeneratedResult generateDelete(QualifiedTableName cassandraTable, Record record,
                                             Map<PrimaryKeyIdentifier, RecordPath> primaryKeyOverrides, List<String> deleteKeyNames) {
        RecordSchema schema = record.getSchema();

        if (deleteKeyNames == null || deleteKeyNames.isEmpty()) {
            throw new IllegalArgumentException("No delete keys were specified");
        }

        // Verify every delete key is either a real record field or covered by a primary key override for this
        // table, matching generateUpdate: a key resolved purely by RecordPath has no same-named record field,
        // but getBindValues can still resolve its value from the override alone.
        final Set<String> overrideFieldNames = overrideFieldNamesForTable(cassandraTable, primaryKeyOverrides);
        for (String deleteKey : deleteKeyNames) {
            if (!isKnownKey(deleteKey, schema, overrideFieldNames)) {
                throw new IllegalArgumentException("Delete key '" + deleteKey + "' is not present in the record schema");
            }
        }

        DeleteSelection deleteSelection = cassandraTable.isQualified()
                ? QueryBuilder.deleteFrom(cassandraTable.keyspace(), cassandraTable.table())
                : QueryBuilder.deleteFrom(cassandraTable.table());

        List<Relation> whereCriteria = new ArrayList<>();
        List<String> keysUsedInOrder = new ArrayList<>();

        for (String fieldName : deleteKeyNames) {
            whereCriteria.add(Relation.column(fieldName).isEqualTo(QueryBuilder.bindMarker(fieldName)));
            keysUsedInOrder.add(fieldName);
        }

        return new GeneratedResult(deleteSelection.where(whereCriteria).build(), keysUsedInOrder);
    }

    protected GeneratedResult generateUpdate(QualifiedTableName cassandraTable, Record record, Map<PrimaryKeyIdentifier, RecordPath> primaryKeyOverrides,
                                              List<String> updateKeyNames, UpdateMethod updateMethod, Integer ttlSeconds, String timestampField) {
        RecordSchema schema = record.getSchema();

        List<String> keysUsedInOrder = new ArrayList<>();

        if (updateKeyNames == null || updateKeyNames.isEmpty()) {
            throw new IllegalArgumentException("No Update Keys were specified");
        }

        // Verify every update key is either a real record field or covered by a primary key override for
        // this table - an update key with neither, such as a date column derived from a timestamp field via
        // a format() RecordPath override, would otherwise be rejected here even though getBindValues can
        // resolve its value perfectly well from the override alone.
        final Set<String> overrideFieldNames = overrideFieldNamesForTable(cassandraTable, primaryKeyOverrides);
        for (String updateKey : updateKeyNames) {
            if (!isKnownKey(updateKey, schema, overrideFieldNames)) {
                throw new IllegalArgumentException("Update key '" + updateKey + "' is not present in the record schema");
            }
        }

        UpdateStart updateQueryStart = cassandraTable.isQualified()
                ? QueryBuilder.update(cassandraTable.keyspace(), cassandraTable.table())
                : QueryBuilder.update(cassandraTable.table());

        // Cassandra/ScyllaDB do not support a TTL or a custom write timestamp on counter columns, so both are
        // only applied for SET updates.
        if (ttlSeconds != null && updateMethod == UpdateMethod.SET) {
            updateQueryStart = updateQueryStart.usingTtl(ttlSeconds);
        }
        if (timestampField != null && updateMethod == UpdateMethod.SET) {
            updateQueryStart = updateQueryStart.usingTimestamp(QueryBuilder.bindMarker(WRITE_TIMESTAMP_BIND_MARKER));
        }

        UpdateWithAssignments updateAssignments = null;

        final String timestampOnlyField = updateMethod == UpdateMethod.SET ? timestampOnlyField(cassandraTable, timestampField) : null;
        List<String> otherKeys = fieldsToWrite(cassandraTable, record, primaryKeyOverrides).stream()
                .filter(fieldName -> !updateKeyNames.contains(fieldName) && !fieldName.equals(timestampOnlyField))
                .toList();

        for (String fieldName : otherKeys) {
            if (updateMethod == UpdateMethod.SET) {
                updateAssignments = updateAssignments == null ? updateQueryStart.setColumn(fieldName, QueryBuilder.bindMarker(fieldName))
                        : updateAssignments.setColumn(fieldName, QueryBuilder.bindMarker(fieldName));
            } else if (updateMethod == UpdateMethod.INCREMENT) {
                updateAssignments = updateAssignments == null ? updateQueryStart.increment(fieldName, QueryBuilder.bindMarker(fieldName))
                        : updateAssignments.increment(fieldName, QueryBuilder.bindMarker(fieldName));
            } else if (updateMethod == UpdateMethod.DECREMENT) {
                updateAssignments = updateAssignments == null ? updateQueryStart.decrement(fieldName, QueryBuilder.bindMarker(fieldName))
                        : updateAssignments.decrement(fieldName, QueryBuilder.bindMarker(fieldName));
            } else {
                throw new IllegalArgumentException("Update Method '" + updateMethod + "' is not valid.");
            }

            keysUsedInOrder.add(fieldName);
        }

        if (updateAssignments == null) {
            throw new ProcessException("No update assignment found");
        }

        Update update = null;

        for (String fieldName : updateKeyNames) {
            update = update == null ? updateAssignments.whereColumn(fieldName).isEqualTo(QueryBuilder.bindMarker(fieldName))
                    : update.whereColumn(fieldName).isEqualTo(QueryBuilder.bindMarker(fieldName));
            keysUsedInOrder.add(fieldName);
        }

        return new GeneratedResult(update.build(),  keysUsedInOrder);
    }

    @Override
    public void delete(QualifiedTableName cassandraTable, Record record, Map<PrimaryKeyIdentifier, RecordPath> primaryKeyOverrides,
                        List<String> updateKeys) throws QueryFailureException {
        GeneratedResult result = generateDelete(cassandraTable, record, primaryKeyOverrides, updateKeys);
        PreparedStatement preparedStatement = prepare(result.statement());

        Object[] values = getBindValues(cassandraTable, primaryKeyOverrides, record, preparedStatement, result.keysUsed());

        if (getLogger().isDebugEnabled()) {
            getLogger().debug("Executing CQL delete: {}{}", result.statement().getQuery(), valuesFragment(values));
        }

        BoundStatement deleteStatement = encode(preparedStatement, values);

        try {
            cassandraSession.execute(deleteStatement);
        } catch (QueryExecutionException | AllNodesFailedException | DriverTimeoutException qee) {
            getLogger().error("Error executing delete", qee);
            throw new QueryFailureException();
        }
    }

    @Override
    public void update(QualifiedTableName cassandraTable, Record record, Map<PrimaryKeyIdentifier, RecordPath> primaryKeyOverrides,
                        List<String> updateKeys, UpdateMethod updateMethod, WriteOverrides overrides) throws QueryFailureException {
        final boolean useTimestampOverride = hasTimestampOverride(overrides) && updateMethod == UpdateMethod.SET;
        GeneratedResult result = generateUpdate(cassandraTable, record, primaryKeyOverrides, updateKeys, updateMethod, resolveTtlSeconds(overrides, defaultTtl), useTimestampOverride ? overrides.timestampField() : null);
        PreparedStatement preparedStatement = prepare(result.statement());

        Object[] values = getBindValues(cassandraTable, primaryKeyOverrides, record, preparedStatement, result.keysUsed());
        if (useTimestampOverride) {
            setTimestampBindValue(values, preparedStatement, record, overrides);
        }

        if (getLogger().isDebugEnabled()) {
            getLogger().debug("Executing CQL {} update: {}{}", updateMethod, result.statement().getQuery(), valuesFragment(values));
        }

        BoundStatement statement = bind(preparedStatement, values, overrides);

        try {
            cassandraSession.execute(statement);
        } catch (QueryExecutionException | AllNodesFailedException | DriverTimeoutException qee) {
            getLogger().error("Error executing update", qee);
            throw new QueryFailureException();
        }
    }

    /**
     * Resolves each named field to its actual bind-marker position and converts its record value for that
     * position's CQL type, leaving any position not named in {@code keyNamesInOrder} for the caller to set.
     */
    private Object[] getBindValues(QualifiedTableName qualifiedTableName,
                                   Map<PrimaryKeyIdentifier, RecordPath> mappings,
                                   Record record,
                                   PreparedStatement preparedStatement, List<String> keyNamesInOrder) {
        ColumnDefinitions variableDefinitions = preparedStatement.getVariableDefinitions();
        Object[] result = new Object[variableDefinitions.size()];

        for (String fieldName : keyNamesInOrder) {
            int index = variableDefinitions.firstIndexOf(fieldName);

            RecordPath path = getRecordPathOverride(qualifiedTableName, fieldName, mappings);
            Object value;
            if (path != null) {
                value = evaluateOverride(record, path);
            } else {
                value = record.getValue(fieldName);
            }
            DataType cqlType = variableDefinitions.get(index).getType();
            result[index] = convertForBind(value, cqlType, "Field '" + fieldName + "'");
        }

        return result;
    }

    /**
     * Converts each positional parameter to the Java type its bind marker's declared CQL type requires, failing
     * fast if the parameter count doesn't match the statement's bind markers.
     */
    private Object[] toBindValues(final PreparedStatement preparedStatement, final List<Object> parameters) {
        final ColumnDefinitions bindMarkers = preparedStatement.getVariableDefinitions();

        if (bindMarkers.size() != parameters.size()) {
            throw new IllegalArgumentException(String.format(
                    "Query declares %d bind marker(s) but %d parameter(s) were supplied",
                    bindMarkers.size(), parameters.size()));
        }

        final Object[] values = new Object[parameters.size()];

        for (int index = 0; index < parameters.size(); index++) {
            final DataType cqlType = bindMarkers.get(index).getType();
            final Object value = parameters.get(index);

            values[index] = convertForBind(value, cqlType, "Parameter " + (index + 1));
        }

        return values;
    }

    /**
     * Converts a value bound to a {@code timestamp} column into the {@link Instant} the driver's own codec
     * expects. Unlike the integer types, {@code timestamp} has no {@code Flexible*Codec} registered to coerce a
     * compatible-but-non-exact value, so ISO-8601 text or epoch milliseconds - the natural forms for a parameter,
     * or for a record whose schema was inferred - would otherwise fail with the driver's opaque
     * {@code CodecNotFoundException}.
     */
    static Instant toInstant(final Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Instant instantValue) {
            return instantValue;
        } else if (value instanceof java.util.Date dateValue) {
            // Through the epoch milliseconds: java.sql.Date throws from toInstant().
            return Instant.ofEpochMilli(dateValue.getTime());
        } else if (value instanceof Number numberValue) {
            return Instant.ofEpochMilli(ExactNumbers.toEpochMillis(numberValue));
        }

        final String text = value.toString().trim();

        try {
            return Instant.parse(text);
        } catch (final DateTimeParseException e) {
            try {
                return Instant.ofEpochMilli(Long.parseLong(text));
            } catch (final NumberFormatException ignored) {
                throw new RejectedValueException(String.format(
                        "Value%s cannot be bound to a timestamp column: expected an ISO-8601 instant such as "
                                + "2026-08-07T14:30:00Z, or a count of milliseconds since the epoch", shown(text)), e);
            }
        }
    }

    /**
     * Binds a record's values, leaving each null one unset when the caller asked for that. An unset value is
     * not sent, so the column keeps whatever it holds; a bound null deletes the column's value and leaves a
     * tombstone. A primary key column cannot be unset, and the cluster rejects the statement if one is.
     */
    static BoundStatement bind(final PreparedStatement preparedStatement, final Object[] values, final WriteOverrides overrides) {
        BoundStatement boundStatement = encode(preparedStatement, values);

        if (overrides != null && overrides.unsetNulls()) {
            for (int index = 0; index < values.length; index++) {
                if (values[index] == null) {
                    boundStatement = boundStatement.unset(index);
                }
            }
        }

        return boundStatement;
    }

    private Object evaluateOverride(Record record, RecordPath path) {
        RecordPathResult result = path.evaluate(record);
        List<FieldValue> valueList = result.getSelectedFields().toList();

        if (valueList.isEmpty()) {
            throw new IllegalArgumentException(String.format("%s evaluated to no values.", path.getPath()));
        } else if (valueList.size() > 1) {
            throw new IllegalArgumentException(String.format("%s evaluated to more than one value.", path.getPath()));
        }

        return valueList.get(0).getValue();
    }

    private RecordPath getRecordPathOverride(QualifiedTableName tableName, String fieldName,
                                          Map<PrimaryKeyIdentifier, RecordPath> overrides) {
        final String keyspace = tableName.isQualified() ? tableName.keyspace() : defaultKeyspace;
        if (keyspace == null) {
            return null;
        }

        // All three parts are compared as CQL identifiers, so an unquoted name matches whatever its case.
        Optional<Map.Entry<PrimaryKeyIdentifier, RecordPath>> identifier = overrides
                .entrySet()
                .stream()
                .filter((e) -> sameCqlName(e.getKey().keyspace(), keyspace)
                        && sameCqlName(e.getKey().tableName(), tableName.table())
                        && sameCqlName(e.getKey().fieldName(), fieldName))
                .findFirst();

        return identifier.map(Map.Entry::getValue).orElse(null);
    }

    private static boolean sameCqlName(final String first, final String second) {
        return CqlIdentifier.fromCql(first).equals(CqlIdentifier.fromCql(second));
    }

    /**
     * Fills in the slot reserved for {@link #WRITE_TIMESTAMP_BIND_MARKER} directly in the same array used for
     * the single {@link PreparedStatement#bind(Object...)} call, rather than binding it separately afterward via
     * a named setter on the resulting {@link BoundStatement} - the latter looked correct but did not reliably
     * take effect for UPDATE statements when verified against a real cluster.
     */
    private void setTimestampBindValue(Object[] values, PreparedStatement preparedStatement, Record record, WriteOverrides overrides) {
        int index = preparedStatement.getVariableDefinitions().firstIndexOf(WRITE_TIMESTAMP_BIND_MARKER);
        values[index] = toEpochMicros(record.getValue(overrides.timestampField()));
    }

    /** {@link #convertForCqlType}, with any failure reported against {@code target}, such as a field name. */
    private Object convertForBind(final Object value, final DataType cqlType, final String target) {
        try {
            return convertForCqlType(value, cqlType);
        } catch (final RuntimeException e) {
            throw conversionFailure(target + " cannot be bound to its " + cqlType.asCql(false, true) + " column", e);
        }
    }

    /** Binds values that have been converted, encoding each with its column's codec, which can also reject one. */
    private static BoundStatement encode(final PreparedStatement preparedStatement, final Object[] values) {
        try {
            return preparedStatement.bind(values);
        } catch (final CodecNotFoundException e) {
            // Names only the CQL and Java types, never the value.
            throw e;
        } catch (final RuntimeException e) {
            throw conversionFailure(rejectedMarker(preparedStatement, values), e);
        }
    }

    /**
     * Which bind marker's value the codecs rejected, found by binding each value on its own. Only reached once a
     * bind has failed, so the extra binds cost nothing on the normal path.
     */
    private static String rejectedMarker(final PreparedStatement preparedStatement, final Object[] values) {
        for (int index = 0; index < values.length; index++) {
            final Object[] single = new Object[index + 1];
            single[index] = values[index];
            try {
                preparedStatement.bind(single);
            } catch (final RuntimeException e) {
                final ColumnDefinition marker = preparedStatement.getVariableDefinitions().get(index);
                return String.format("Bind marker %d ('%s') cannot be bound to its %s column",
                        index + 1, marker.getName().asInternal(), marker.getType().asCql(false, true));
            }
        }
        return "A value cannot be encoded for its column";
    }

    /**
     * The error for a value that could not be converted. This service's own messages leave the value out unless
     * value logging is on, so they are kept. Others, NiFi's and the driver's, can quote the value, so with value
     * logging off they are reduced to their type and dropped as a cause.
     */
    static IllegalArgumentException conversionFailure(final String context, final RuntimeException e) {
        if (e instanceof RejectedValueException || VALUE_LOGGING_ENABLED) {
            return new RejectedValueException(context + ": " + e.getMessage(), e);
        }
        return new RejectedValueException(context + ": " + e.getClass().getSimpleName()
                + ". Set ENABLE_CQL_VALUE_LOGGING=true to include the value and the full error.", null);
    }

    private Object convertForCqlType(final Object value, final DataType cqlType) {
        if (value == null) {
            return null;
        }

        // A RECORD-typed value is normally a nested Record, but DataTypeUtils#toRecord also accepts a raw Map
        // as an equally valid representation, so both are converted into a UdtValue the same way here.
        if (cqlType instanceof UserDefinedType udtType
                && (value instanceof Record || value instanceof Map<?, ?>)) {
            final Function<String, Object> fieldValueLookup = value instanceof Record nestedRecord
                    ? nestedRecord::getValue
                    : ((Map<?, ?>) value)::get;

            UdtValue udtValue = udtType.newValue();
            final List<String> fieldNames = udtType.getFieldNames().stream().map(name -> name.asInternal()).toList();

            for (int i = 0; i < fieldNames.size(); i++) {
                final String fieldName = fieldNames.get(i);
                final DataType fieldCqlType = udtType.getFieldTypes().get(i);
                try {
                    final Object fieldValue = convertForCqlType(fieldValueLookup.apply(fieldName), fieldCqlType);
                    // The codec is looked up from the value, as for a tuple element, so the fallback codecs that
                    // bind text or a narrower number are found; a UDT read from the schema carries the session's
                    // registry, where they are registered. A null field is set against its declared type, since
                    // there is no value to look a codec up from.
                    udtValue = fieldValue == null
                            ? udtValue.setToNull(i)
                            : udtValue.set(i, fieldValue, udtValue.codecRegistry().codecFor(fieldCqlType, fieldValue));
                } catch (final RuntimeException e) {
                    throw conversionFailure("UDT field '" + fieldName + "' cannot be bound to its " + fieldCqlType.asCql(false, true) + " type", e);
                }
            }

            return udtValue;
        }

        // An ARRAY-typed value is normally Object[], NiFi's own canonical representation (DataTypeUtils#toArray
        // always returns one), but any collection is accepted too, for list and set columns alike.
        if (cqlType instanceof ListType listType && (value instanceof Collection<?> || value instanceof Object[])) {
            final DataType elementCqlType = listType.getElementType();
            return toElementStream(value).map(element -> convertForCqlType(element, elementCqlType)).toList();
        }

        if (cqlType instanceof SetType setType && (value instanceof Collection<?> || value instanceof Object[])) {
            final DataType elementCqlType = setType.getElementType();
            return toElementStream(value).map(element -> convertForCqlType(element, elementCqlType)).collect(Collectors.toSet());
        }

        if (value instanceof Map<?, ?> map && cqlType instanceof MapType mapType) {
            final DataType keyCqlType = mapType.getKeyType();
            final DataType valueCqlType = mapType.getValueType();
            final Map<Object, Object> converted = new HashMap<>();

            for (final Map.Entry<?, ?> entry : map.entrySet()) {
                converted.put(convertForCqlType(entry.getKey(), keyCqlType), convertForCqlType(entry.getValue(), valueCqlType));
            }

            return converted;
        }

        if (cqlType instanceof TupleType tupleType) {
            return toTupleValue(value, tupleType);
        }

        if (cqlType.equals(DataTypes.UUID)) {
            return DataTypeUtils.toUUID(value);
        }

        // The driver has no codec for these from the forms a record or a parameter commonly holds - bytes as
        // byte[] or Object[], and the rest as text or another number type - so each is converted here.
        if (cqlType.equals(DataTypes.BLOB)) {
            return toByteBuffer(value);
        }
        if (cqlType.equals(DataTypes.TIMESTAMP)) {
            return toInstant(value);
        }
        if (cqlType.equals(DataTypes.DECIMAL)) {
            return toBigDecimal(value);
        }
        if (cqlType.equals(DataTypes.VARINT)) {
            return toBigInteger(value);
        }
        if (cqlType.equals(DataTypes.INET)) {
            return toInetAddress(value);
        }
        // The counter codec only binds a number, and an ExecuteCQL parameter is always text.
        if (cqlType.equals(DataTypes.COUNTER)) {
            return value instanceof Number ? value : parseNumber(value, Long::valueOf, "counter");
        }
        if (cqlType.equals(DataTypes.DURATION)) {
            return toCqlDuration(value);
        }

        // The driver's own codec already rejects a non-version-1 UUID bound to a timeuuid column, but only
        // via a confusing CodecNotFoundException ("Codec not found for requested operation: [TIMEUUID <->
        // java.util.UUID]") that reads like a driver/configuration problem rather than a data problem.
        // Checking here instead produces a clear, attributable error before the value ever reaches bind().
        if (cqlType.equals(DataTypes.TIMEUUID)) {
            final UUID uuid = DataTypeUtils.toUUID(value);
            if (uuid.version() != 1) {
                throw new RejectedValueException(String.format(
                        "Value%s is not a valid timeuuid: version %d, but timeuuid columns require a version 1 (time-based) UUID",
                        shown(uuid), uuid.version()), null);
            }
            return uuid;
        }

        return value;
    }

    /**
     * A tuple value from its elements, as an {@code Object[]} or collection, or from its CQL literal text, such
     * as {@code (7,'x')}, which is the form a tuple column is read as.
     */
    private TupleValue toTupleValue(final Object value, final TupleType tupleType) {
        if (value instanceof TupleValue tuple) {
            return tuple;
        }

        final CodecRegistry codecs = cassandraSession.getContext().getCodecRegistry();

        if (!(value instanceof Object[] || value instanceof Collection<?>)) {
            try {
                return (TupleValue) codecs.codecFor(tupleType).parse(value.toString().trim());
            } catch (final IllegalArgumentException e) {
                throw new RejectedValueException(String.format("Value%s cannot be bound to a %s column: it is not a tuple literal",
                        shown(value), tupleType.asCql(false, true)), e);
            }
        }

        final List<?> elements = toElementStream(value).toList();
        final List<DataType> componentTypes = tupleType.getComponentTypes();
        if (elements.size() != componentTypes.size()) {
            throw new RejectedValueException(String.format("A %s column needs %d elements, but the value has %d",
                    tupleType.asCql(false, true), componentTypes.size(), elements.size()), null);
        }

        TupleValue tuple = tupleType.newValue();
        for (int i = 0; i < elements.size(); i++) {
            final Object element = convertForCqlType(elements.get(i), componentTypes.get(i));
            // The codec is looked up from the value, not its class, so the fallback codecs that bind text to a
            // number column are found. A null element is set against its declared type instead.
            tuple = element == null ? tuple.setToNull(i) : tuple.set(i, element, codecs.codecFor(componentTypes.get(i), element));
        }
        return tuple;
    }

    /** A duration from a {@link CqlDuration}, as a duration column is read, or from its text, such as {@code 1h30m}. */
    private static CqlDuration toCqlDuration(final Object value) {
        if (value instanceof CqlDuration duration) {
            return duration;
        }

        final String text = value.toString().trim();
        try {
            return CqlDuration.from(text);
        } catch (final IllegalArgumentException e) {
            throw new RejectedValueException(String.format("Value%s cannot be bound to a duration column: it is not a duration such as 1h30m or PT1H30M", shown(text)), e);
        }
    }

    private static Stream<?> toElementStream(final Object value) {
        return (value instanceof Object[] array) ? Arrays.stream(array) : ((Collection<?>) value).stream();
    }

    /** Bytes for a blob column, from a {@code byte[]}, or an {@code Object[]} or collection of bytes. */
    private static Object toByteBuffer(final Object value) {
        return switch (value) {
            case byte[] bytes -> ByteBuffer.wrap(bytes);
            case Object[] elements -> ByteBuffer.wrap(toBytes(elements));
            case Collection<?> elements -> ByteBuffer.wrap(toBytes(elements.toArray()));
            default -> value;
        };
    }

    /**
     * Bytes from numbers, each a whole number from -128 to 255: the signed range a {@code Byte} holds, or the
     * unsigned one byte values are often written in, so 200 and -56 are the same byte. Anything else is rejected
     * rather than narrowed, which would store 300 as 44.
     */
    private static byte[] toBytes(final Object[] elements) {
        final byte[] bytes = new byte[elements.length];
        for (int i = 0; i < elements.length; i++) {
            final long number = elements[i] instanceof Number value ? toWholeNumber(value) : Long.MIN_VALUE;
            if (number < Byte.MIN_VALUE || number > 255) {
                throw new RejectedValueException(String.format(
                        "Value cannot be bound to a blob column: element %d%s is not a byte, a whole number from -128 to 255",
                        i, shown(elements[i])), null);
            }
            bytes[i] = (byte) number;
        }
        return bytes;
    }

    /** A blob element as a whole number, or {@link Long#MIN_VALUE} when it has a fraction or is out of range. */
    private static long toWholeNumber(final Number value) {
        try {
            return ExactNumbers.toLong(value, "blob");
        } catch (final RejectedValueException e) {
            return Long.MIN_VALUE;
        }
    }

    private static BigDecimal toBigDecimal(final Object value) {
        return switch (value) {
            case BigDecimal decimal -> decimal;
            case BigInteger integer -> new BigDecimal(integer);
            // Through the shortest decimal text that round-trips, so 1.1 binds as 1.1 rather than 1.100000000000000088...
            case Double number -> new BigDecimal(number.toString());
            case Float number -> new BigDecimal(number.toString());
            case Number number -> BigDecimal.valueOf(number.longValue());
            default -> parseNumber(value, BigDecimal::new, "decimal");
        };
    }

    private static BigInteger toBigInteger(final Object value) {
        return switch (value) {
            case BigInteger integer -> integer;
            case BigDecimal decimal -> wholeNumber(decimal, value);
            case Double number -> wholeNumber(new BigDecimal(number.toString()), value);
            case Float number -> wholeNumber(new BigDecimal(number.toString()), value);
            case Number number -> BigInteger.valueOf(number.longValue());
            default -> parseNumber(value, BigInteger::new, "varint");
        };
    }

    private static BigInteger wholeNumber(final BigDecimal decimal, final Object value) {
        try {
            return decimal.toBigIntegerExact();
        } catch (final ArithmeticException e) {
            throw new RejectedValueException(String.format("Value%s cannot be bound to a varint column: it is not a whole number", shown(value)), e);
        }
    }

    private static <T> T parseNumber(final Object value, final Function<String, T> parser, final String cqlType) {
        final String text = value.toString().trim();
        try {
            return parser.apply(text);
        } catch (final NumberFormatException e) {
            throw new RejectedValueException(String.format("Value%s cannot be bound to a %s column: it is not a number", shown(text), cqlType), e);
        }
    }

    private static final Pattern IPV4_LITERAL = Pattern.compile("(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})");

    /**
     * An inet value from an address literal. A host name is rejected rather than resolved: resolving it would be
     * a DNS lookup per value, and the address would depend on where and when the flow ran.
     */
    private static InetAddress toInetAddress(final Object value) {
        if (value instanceof InetAddress address) {
            return address;
        }

        final String text = value.toString().trim();
        try {
            final Matcher ipv4 = IPV4_LITERAL.matcher(text);
            if (ipv4.matches()) {
                final byte[] octets = new byte[4];
                for (int i = 0; i < octets.length; i++) {
                    final int octet = Integer.parseInt(ipv4.group(i + 1));
                    if (octet > 255) {
                        throw new UnknownHostException(text);
                    }
                    octets[i] = (byte) octet;
                }
                return InetAddress.getByAddress(octets);
            }
            // A host name cannot contain a colon, so getByName parses this as an IPv6 literal, failing rather
            // than falling back to a lookup when it is not one.
            if (text.contains(":")) {
                return InetAddress.getByName(text);
            }
        } catch (final UnknownHostException e) {
            // reported below, the same as text that is no address at all
        }

        throw new RejectedValueException(String.format(
                "Value%s cannot be bound to an inet column: expected an IPv4 or IPv6 address literal", shown(text)), null);
    }

    @Override
    public void update(QualifiedTableName cassandraTable, List<Record> records, Map<PrimaryKeyIdentifier, RecordPath> primaryKeyOverrides,
                        List<String> updateKeys, UpdateMethod updateMethod, CqlBatchType batchType,
                        WriteOverrides overrides) throws QueryFailureException {
        if (records == null || records.isEmpty()) {
            return;
        }

        final boolean useTimestampOverride = hasTimestampOverride(overrides) && updateMethod == UpdateMethod.SET;
        BatchStatementBuilder builder = BatchStatement.builder(toDriverBatchType(batchType));

        GeneratedResult result = generateUpdate(cassandraTable, records.get(0), primaryKeyOverrides, updateKeys, updateMethod, resolveTtlSeconds(overrides, defaultTtl), useTimestampOverride ? overrides.timestampField() : null);
        PreparedStatement preparedStatement = prepare(result.statement());

        if (getLogger().isDebugEnabled()) {
            getLogger().debug("Executing CQL {} batch {} update of {} record(s): {}", batchType, updateMethod, records.size(), result.statement().getQuery());
        }

        int recordNumber = 0;
        for (Record record : records) {
            recordNumber++;
            Object[] values = getBindValues(cassandraTable, primaryKeyOverrides, record, preparedStatement, result.keysUsed());
            if (useTimestampOverride) {
                setTimestampBindValue(values, preparedStatement, record, overrides);
            }

            if (VALUE_LOGGING_ENABLED && getLogger().isDebugEnabled()) {
                getLogger().debug("Batch update record {}{}", recordNumber, valuesFragment(values));
            }

            builder.addStatement(bind(preparedStatement, values, overrides));
        }

        try {
            cassandraSession.execute(builder.build());
        } catch (QueryExecutionException | AllNodesFailedException | DriverTimeoutException qee) {
            getLogger().error("Error executing batch update", qee);
            throw new QueryFailureException();
        }
    }

    /**
     * Read from the driver's schema metadata on every call. The driver already holds that metadata in memory and
     * refreshes it when the schema changes, so a second cache here could only go stale.
     */
    @Override
    public PrimaryKey getMetadata(QualifiedTableName table) {
        final QualifiedTableName resolved = resolveKeyspace(table);
        if (!resolved.isQualified()) {
            throw new IllegalArgumentException("Table '" + table.table() + "' names no keyspace and the service has no Default Keyspace");
        }

        final KeyspaceMetadata keyspaceMetadata = cassandraSession.getMetadata().getKeyspace(resolved.keyspace())
                .orElseThrow(() -> new IllegalArgumentException("Keyspace '" + resolved.keyspace() + "' was not found in the cluster"));
        final TableMetadata tableMetadata = keyspaceMetadata.getTable(resolved.table())
                .orElseThrow(() -> new IllegalArgumentException(
                        "Table '" + resolved.table() + "' was not found in keyspace '" + resolved.keyspace() + "'"));

        return convertTableMetadata(tableMetadata);
    }

    @Override
    public String getDefaultKeyspace() {
        return this.defaultKeyspace;
    }

    record GeneratedResult(SimpleStatement statement, List<String> keysUsed) {

    }
}
