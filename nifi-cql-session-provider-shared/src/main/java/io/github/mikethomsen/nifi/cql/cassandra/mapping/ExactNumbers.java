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

package io.github.mikethomsen.nifi.cql.cassandra.mapping;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

import static io.github.mikethomsen.nifi.cql.cassandra.mapping.RejectedValueException.shown;

/**
 * Number conversions for binding that never change a value silently. Java's own narrowing, which NiFi's
 * {@code DataTypeUtils} uses, keeps the low bits of an integer and drops the fraction of a decimal, so 40000
 * becomes -25536 in a smallint and 3.7 becomes 3 in an int. These reject such a value instead, naming the CQL
 * type it was meant for.
 */
public final class ExactNumbers {

    private ExactNumbers() {
    }

    /** @throws RejectedValueException if {@code value} has a fraction or does not fit in a long */
    public static long toLong(final Number value, final String cqlType) {
        return switch (value) {
            case Byte number -> number;
            case Short number -> number;
            case Integer number -> number;
            case Long number -> number;
            default -> wholeNumber(toBigDecimal(value, cqlType), value, cqlType);
        };
    }

    public static int toInt(final Number value, final String cqlType) {
        return (int) inRange(toLong(value, cqlType), Integer.MIN_VALUE, Integer.MAX_VALUE, value, cqlType);
    }

    public static short toShort(final Number value, final String cqlType) {
        return (short) inRange(toLong(value, cqlType), Short.MIN_VALUE, Short.MAX_VALUE, value, cqlType);
    }

    public static byte toByte(final Number value, final String cqlType) {
        return (byte) inRange(toLong(value, cqlType), Byte.MIN_VALUE, Byte.MAX_VALUE, value, cqlType);
    }

    /** Rounds to the nearest float, which is what a float column is; rejects a finite value too large for one. */
    public static float toFloat(final Number value, final String cqlType) {
        return checkFloat(value.floatValue(), value, cqlType);
    }

    /** Rounds to the nearest double; rejects a finite value too large for one. */
    public static double toDouble(final Number value, final String cqlType) {
        return checkDouble(value.doubleValue(), value, cqlType);
    }

    /**
     * Rejects a float that came out infinite from an input that was not, as Java's parsers and narrowing both
     * return for a value out of range. An infinity or NaN given as such is kept, since CQL can store it.
     */
    public static float checkFloat(final float result, final Object input, final String cqlType) {
        if (Float.isInfinite(result) && !isInfinity(input)) {
            throw outOfRange(input, cqlType);
        }
        return result;
    }

    /** As {@link #checkFloat}, for a double. */
    public static double checkDouble(final double result, final Object input, final String cqlType) {
        if (Double.isInfinite(result) && !isInfinity(input)) {
            throw outOfRange(input, cqlType);
        }
        return result;
    }

    /**
     * Whole milliseconds from a number of milliseconds since the epoch, for a timestamp. A fraction of a
     * millisecond is dropped, since a CQL timestamp cannot hold one; a value that does not fit in a long is rejected.
     */
    public static long toEpochMillis(final Number value) {
        final BigDecimal decimal = toBigDecimal(value, "timestamp");
        try {
            return decimal.setScale(0, RoundingMode.DOWN).longValueExact();
        } catch (final ArithmeticException e) {
            throw outOfRange(value, "timestamp");
        }
    }

    private static BigDecimal toBigDecimal(final Number value, final String cqlType) {
        return switch (value) {
            case BigDecimal decimal -> decimal;
            case BigInteger integer -> new BigDecimal(integer);
            case Double number when !Double.isFinite(number) -> throw notFinite(value, cqlType);
            case Float number when !Float.isFinite(number) -> throw notFinite(value, cqlType);
            // Through the shortest decimal text that round-trips, so 42.0 is whole and 0.1 is exactly 0.1.
            default -> {
                try {
                    yield new BigDecimal(value.toString());
                } catch (final NumberFormatException e) {
                    throw new RejectedValueException(String.format(
                            "Value%s cannot be bound to a %s column: it is not a number", shown(value), cqlType), e);
                }
            }
        };
    }

    private static long wholeNumber(final BigDecimal decimal, final Number value, final String cqlType) {
        try {
            return decimal.longValueExact();
        } catch (final ArithmeticException e) {
            if (decimal.stripTrailingZeros().scale() > 0) {
                throw new RejectedValueException(String.format(
                        "Value%s cannot be bound to a %s column: it is not a whole number", shown(value), cqlType), null);
            }
            throw outOfRange(value, cqlType);
        }
    }

    private static long inRange(final long number, final long min, final long max, final Number value, final String cqlType) {
        if (number < min || number > max) {
            throw outOfRange(value, cqlType);
        }
        return number;
    }

    private static boolean isInfinity(final Object input) {
        return switch (input) {
            case Double number -> number.isInfinite();
            case Float number -> number.isInfinite();
            case String text -> text.trim().matches("[+-]?Infinity");
            case null, default -> false;
        };
    }

    private static RejectedValueException outOfRange(final Object value, final String cqlType) {
        return new RejectedValueException(String.format(
                "Value%s cannot be bound to a %s column: it is out of range for %s", shown(value), cqlType, cqlType), null);
    }

    private static RejectedValueException notFinite(final Object value, final String cqlType) {
        return new RejectedValueException(String.format(
                "Value%s cannot be bound to a %s column: it is not a finite number", shown(value), cqlType), null);
    }
}
