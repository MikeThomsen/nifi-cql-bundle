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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import io.github.mikethomsen.nifi.cql.api.metadata.QualifiedTableName;
import io.github.mikethomsen.nifi.cql.api.service.WriteOverrides;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Covers what the service derives from the driver's cluster metadata - the provenance transit URL and table
 * lookups - and how a record's null values are bound.
 */
class CassandraCQLExecutionServiceMetadataTest {

    private static final QualifiedTableName TABLE = new QualifiedTableName("ks", "message");

    private final CassandraCQLExecutionService service = new CassandraCQLExecutionService();
    private final Metadata metadata = mock(Metadata.class);

    @BeforeEach
    void setUp() {
        final CqlSession session = mock(CqlSession.class);
        when(session.getMetadata()).thenReturn(metadata);
        service.setSession(session);
    }

    @Test
    @DisplayName("The transit URL names the cluster itself, not the Optional the driver reports it in")
    void testTransitUrlUsesClusterName() {
        when(metadata.getClusterName()).thenReturn(Optional.of("Test Cluster"));

        assertEquals("cassandra://Test Cluster.ks.message", service.getTransitUrl(TABLE));
    }

    @Test
    @DisplayName("The transit URL omits the cluster when the driver does not know its name")
    void testTransitUrlWithoutClusterName() {
        when(metadata.getClusterName()).thenReturn(Optional.empty());

        assertEquals("cassandra://ks.message", service.getTransitUrl(TABLE));
    }

    @Test
    @DisplayName("Looking up a table in a keyspace the cluster does not have names the keyspace")
    void testMetadataForMissingKeyspace() {
        when(metadata.getKeyspace("ks")).thenReturn(Optional.empty());

        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> service.getMetadata(TABLE));
        assertTrue(e.getMessage().contains("Keyspace 'ks'"));
    }

    @Test
    @DisplayName("Looking up a table the keyspace does not have names the table")
    void testMetadataForMissingTable() {
        final KeyspaceMetadata keyspace = mock(KeyspaceMetadata.class);
        when(keyspace.getTable("message")).thenReturn(Optional.empty());
        when(metadata.getKeyspace("ks")).thenReturn(Optional.of(keyspace));

        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> service.getMetadata(TABLE));
        assertTrue(e.getMessage().contains("Table 'message'"));
    }

    @Test
    @DisplayName("Looking up an unqualified table with no default keyspace is rejected")
    void testMetadataForUnqualifiedTableWithoutDefaultKeyspace() {
        assertThrows(IllegalArgumentException.class, () -> service.getMetadata(new QualifiedTableName(null, "message")));
    }

    @Test
    @DisplayName("Null values are left unset when asked, and only those")
    void testNullsAreUnsetWhenRequested() {
        final Object[] values = {"john.smith", null, "hello"};
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final BoundStatement bound = mock(BoundStatement.class);
        final BoundStatement withUnset = mock(BoundStatement.class);
        when(preparedStatement.bind(values)).thenReturn(bound);
        when(bound.unset(1)).thenReturn(withUnset);

        assertSame(withUnset, CassandraCQLExecutionService.bind(preparedStatement, values, new WriteOverrides(null, null, true)));
        verify(bound, never()).unset(0);
        verify(bound, never()).unset(2);
    }

    @Test
    @DisplayName("Null values are bound as nulls when not asked to unset them")
    void testNullsAreBoundByDefault() {
        final Object[] values = {"john.smith", null};
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final BoundStatement bound = mock(BoundStatement.class);
        when(preparedStatement.bind(values)).thenReturn(bound);

        assertSame(bound, CassandraCQLExecutionService.bind(preparedStatement, values, WriteOverrides.NONE));
        verify(bound, never()).unset(anyInt());
    }
}
