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
package io.github.mikethomsen.nifi.cql.cache;

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Parses the table and column names the cache splices into its statement text. Each name must be a single CQL
 * identifier, unquoted or double-quoted, so a property value can only ever name something: it cannot carry a
 * second clause, a comment or a different table.
 *
 * <p>The cache cannot use the driver's own identifier parsing, because it is deliberately built without either
 * driver on its classpath, so this follows the CQL grammar directly: an unquoted identifier is a letter followed
 * by letters, digits and underscores, and is case-insensitive; a quoted one is any non-empty text between double
 * quotes, with a double quote inside written twice, and keeps its case.
 */
final class CqlNames {

    private static final Pattern UNQUOTED = Pattern.compile("[A-Za-z][A-Za-z0-9_]*");

    /** Validates a single identifier, such as a column name. Values with Expression Language are checked on enable. */
    static final Validator IDENTIFIER_VALIDATOR = validator(1);

    /** Validates {@code <table>} or {@code <keyspace>.<table>}. Values with Expression Language are checked on enable. */
    static final Validator TABLE_VALIDATOR = validator(2);

    private CqlNames() {
    }

    /**
     * Splits {@code text} into at most {@code maxParts} dot-separated identifiers.
     *
     * @return each identifier as the cluster stores it: lower-cased if it was unquoted, without quotes if it was
     *         quoted
     * @throws IllegalArgumentException if {@code text} is not that many valid identifiers
     */
    static List<String> parse(final String text, final int maxParts) {
        final List<String> names = new ArrayList<>();
        final int length = text.length();
        int i = 0;

        while (true) {
            if (i < length && text.charAt(i) == '"') {
                final StringBuilder name = new StringBuilder();
                i++;
                while (true) {
                    if (i >= length) {
                        throw new IllegalArgumentException("'" + text + "' has a quoted name with no closing quote");
                    }
                    final char c = text.charAt(i++);
                    if (c != '"') {
                        name.append(c);
                    } else if (i < length && text.charAt(i) == '"') {
                        name.append('"');
                        i++;
                    } else {
                        break;
                    }
                }
                if (name.isEmpty()) {
                    throw new IllegalArgumentException("'" + text + "' has an empty quoted name");
                }
                names.add(name.toString());
            } else {
                final int start = i;
                while (i < length && text.charAt(i) != '.') {
                    i++;
                }
                final String name = text.substring(start, i);
                if (!UNQUOTED.matcher(name).matches()) {
                    throw new IllegalArgumentException("'" + text + "' is not a valid CQL name: an unquoted name is a "
                            + "letter followed by letters, digits and underscores; put any other name in double quotes");
                }
                names.add(name.toLowerCase(Locale.ROOT));
            }

            if (i == length) {
                break;
            }
            if (text.charAt(i) != '.') {
                throw new IllegalArgumentException("'" + text + "' has text after a quoted name");
            }
            i++;
        }

        if (names.size() > maxParts) {
            throw new IllegalArgumentException(maxParts == 1
                    ? "'" + text + "' must be a single name"
                    : "'" + text + "' must be <table> or <keyspace>.<table>");
        }
        return names;
    }

    /**
     * {@code name} written so the cluster reads it back exactly. Always quoted, since a bare name that happens to
     * be a reserved word, such as {@code table}, would not parse.
     */
    static String toCql(final String name) {
        return '"' + name.replace("\"", "\"\"") + '"';
    }

    private static Validator validator(final int maxParts) {
        return (subject, input, context) -> {
            final ValidationResult.Builder result = new ValidationResult.Builder().subject(subject).input(input);
            if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
                return result.valid(true).explanation("Expression Language is checked when the service is enabled").build();
            }
            try {
                parse(input == null ? "" : input.trim(), maxParts);
                return result.valid(true).build();
            } catch (final IllegalArgumentException e) {
                return result.valid(false).explanation(e.getMessage()).build();
            }
        };
    }
}
