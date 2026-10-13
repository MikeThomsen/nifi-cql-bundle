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
package org.apache.nifi.service.cql.api.service;

import org.apache.nifi.service.cql.api.constants.CqlStatementType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CqlStatementsTest {

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
            "select * from t|SELECT",
            "  SELECT a FROM t|SELECT",
            "insert into t (a) values (?)|INSERT",
            "UPDATE t SET a = ? WHERE k = ?|UPDATE",
            "delete from t where k = ?|DELETE",
            "/* note */ delete from t where k = ?|DELETE"
    })
    void testTypeOf(final String cql, final CqlStatementType expected) {
        assertEquals(expected, CqlStatements.typeOf(cql));
    }

    @Test
    void testLeadingLineCommentsAreSkipped() {
        assertEquals(CqlStatementType.SELECT, CqlStatements.typeOf("-- note\nselect * from t"));
        assertEquals(CqlStatementType.UPDATE, CqlStatements.typeOf("// note\nupdate t set a = 1 where k = 2"));
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"   ", "create table t (k int primary key)", "drop table t", "begin batch apply batch", "selection"})
    void testTypeOfUnsupported(final String cql) {
        assertNull(CqlStatements.typeOf(cql));
    }

    @Test
    void testWhereClauseDetected() {
        assertTrue(CqlStatements.hasWhereClause("delete from t where k = ?"));
        assertTrue(CqlStatements.hasWhereClause("DELETE FROM t\nWHERE k = ?"));
        assertTrue(CqlStatements.hasWhereClause("update t set a = ? where k = ?"));
    }

    @Test
    void testMissingWhereClause() {
        assertFalse(CqlStatements.hasWhereClause("delete from t"));
        assertFalse(CqlStatements.hasWhereClause("update t set a = ?"));
    }

    @Test
    void testWhereInsideLiteralsIdentifiersAndCommentsIsIgnored() {
        assertFalse(CqlStatements.hasWhereClause("update t set a = 'where' "));
        assertFalse(CqlStatements.hasWhereClause("update t set a = 'it''s where' "));
        assertFalse(CqlStatements.hasWhereClause("update \"where\" set a = ?"));
        assertFalse(CqlStatements.hasWhereClause("update t set a = $$ where $$"));
        assertFalse(CqlStatements.hasWhereClause("delete from t -- where k = 1"));
        assertFalse(CqlStatements.hasWhereClause("delete from t /* where k = 1 */"));
    }

    @Test
    void testWhereAsPartOfAWordIsIgnored() {
        assertFalse(CqlStatements.hasWhereClause("update t set somewhere = ?"));
        assertFalse(CqlStatements.hasWhereClause("delete from where_t"));
    }

    @Test
    void testStatementAfterLiteralStillHasWhere() {
        assertTrue(CqlStatements.hasWhereClause("update t set a = 'x' where k = ?"));
    }
}
