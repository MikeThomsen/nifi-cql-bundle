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
package io.github.mikethomsen.nifi.cql.api.service;

import io.github.mikethomsen.nifi.cql.api.constants.CqlStatementType;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Driver-independent, text-level checks on a CQL statement. Needing no cluster and no driver, they can run when
 * a processor is configured as well as when a service validates a statement against a live schema.
 *
 * <p>The scan understands just enough CQL lexing to not be fooled: string literals ({@code '...'} with
 * {@code ''} as an escaped quote, and {@code $$...$$}), quoted identifiers ({@code "..."}), and the three
 * comment forms ({@code --}, {@code //}, {@code /* ... *}{@code /}). A keyword that appears only inside one of
 * those is not a keyword.
 */
public final class CqlStatements {

    private CqlStatements() {
    }

    /**
     * @return the kind of statement {@code cql} is, from its first keyword, or {@code null} if that is not
     * one of {@link CqlStatementType}'s values (or the text is empty)
     */
    public static CqlStatementType typeOf(final String cql) {
        final List<String> words = words(cql);
        if (words.isEmpty()) {
            return null;
        }

        try {
            return CqlStatementType.valueOf(words.getFirst());
        } catch (final IllegalArgumentException e) {
            return null;
        }
    }

    /**
     * @return whether {@code cql} contains a {@code WHERE} keyword outside any literal, identifier or comment
     */
    public static boolean hasWhereClause(final String cql) {
        return words(cql).contains("WHERE");
    }

    /**
     * Splits {@code cql} into its upper-cased bare words, dropping literals, quoted identifiers and comments.
     */
    private static List<String> words(final String cql) {
        final List<String> words = new ArrayList<>();
        if (cql == null) {
            return words;
        }

        final int length = cql.length();
        final StringBuilder word = new StringBuilder();
        int i = 0;

        while (i < length) {
            final char c = cql.charAt(i);

            if (c == '\'') {
                i = skipDelimited(cql, i, "'", true);
            } else if (c == '"') {
                i = skipDelimited(cql, i, "\"", true);
            } else if (cql.startsWith("$$", i)) {
                i = skipDelimited(cql, i, "$$", false);
            } else if (cql.startsWith("--", i) || cql.startsWith("//", i)) {
                final int end = cql.indexOf('\n', i);
                i = end < 0 ? length : end;
            } else if (cql.startsWith("/*", i)) {
                final int end = cql.indexOf("*/", i + 2);
                i = end < 0 ? length : end + 2;
            } else {
                if (Character.isLetterOrDigit(c) || c == '_') {
                    word.append(c);
                } else if (!word.isEmpty()) {
                    words.add(word.toString().toUpperCase(Locale.ROOT));
                    word.setLength(0);
                }
                i++;
                continue;
            }

            // A literal, identifier or comment ends any word in progress, exactly as whitespace would.
            if (!word.isEmpty()) {
                words.add(word.toString().toUpperCase(Locale.ROOT));
                word.setLength(0);
            }
        }

        if (!word.isEmpty()) {
            words.add(word.toString().toUpperCase(Locale.ROOT));
        }

        return words;
    }

    /**
     * @param start index of the opening delimiter
     * @param doubledEscapes whether the delimiter doubled ({@code ''}) stands for itself inside the span
     * @return the index just past the closing delimiter, or the text length if it is never closed
     */
    private static int skipDelimited(final String cql, final int start, final String delimiter, final boolean doubledEscapes) {
        int i = start + delimiter.length();

        while (i < cql.length()) {
            if (cql.startsWith(delimiter, i)) {
                if (doubledEscapes && cql.startsWith(delimiter, i + delimiter.length())) {
                    i += 2 * delimiter.length();
                    continue;
                }
                return i + delimiter.length();
            }
            i++;
        }

        return cql.length();
    }
}
