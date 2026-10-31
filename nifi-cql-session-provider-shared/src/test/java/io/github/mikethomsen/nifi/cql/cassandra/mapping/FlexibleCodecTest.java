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

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import org.apache.nifi.serialization.record.util.IllegalTypeConversionException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * Covers the {@code Flexible*Codec} family's reason for existing.
 *
 * <p>Each of these codecs wraps a built-in driver codec that binds one exact Java type, and widens it to accept
 * anything {@code DataTypeUtils} considers compatible - a numeric String where an int is expected, a Long where
 * a short is expected, and so on. The integration suite writes one value of one Java type per CQL column, so it
 * only ever walks the canonical path; the widening is what these tests cover.
 *
 * <p>The assertion throughout is that the flexible codec produces <em>byte-for-byte the same encoding</em> as
 * the built-in codec given the canonical value. That is the property that matters: a record field arriving as
 * a String must land in the column indistinguishably from one arriving as an Integer.
 */
class FlexibleCodecTest {

    private static final ProtocolVersion VERSION = ProtocolVersion.DEFAULT;

    // ---------------------------------------------------------------------------------------------------
    // The contract every codec in the family shares
    // ---------------------------------------------------------------------------------------------------

    static Stream<Arguments> allCodecs() {
        return Stream.of(
                arguments("FlexibleIntCodec", new FlexibleIntCodec()),
                arguments("FlexibleBigIntCodec", new FlexibleBigIntCodec()),
                arguments("FlexibleSmallIntCodec", new FlexibleSmallIntCodec()),
                arguments("FlexibleTinyIntCodec", new FlexibleTinyIntCodec()),
                arguments("FlexibleDoubleCodec", new FlexibleDoubleCodec()),
                arguments("FlexibleFloatCodec", new FlexibleFloatCodec()),
                arguments("FlexibleBooleanCodec", new FlexibleBooleanCodec()),
                arguments("FlexibleDateCodec", new FlexibleDateCodec()),
                arguments("FlexibleTimeCodec", new FlexibleTimeCodec()),
                arguments("FlexibleCounterCodec", new FlexibleCounterCodec()));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allCodecs")
    @DisplayName("Null encodes to null, formats as the CQL NULL literal and parses back to null")
    void testNullHandling(final String name, final TypeCodec<?> codec) {
        // An absent record field writes nothing rather than failing.
        assertNull(encode(codec, null));
        assertEquals("NULL", format(codec, null));
        // parse() delegates straight to the wrapped codec with no conversion of its own. Asserting it here
        // is what would catch the delegate being swapped for one that reads a different CQL type.
        assertNull(codec.parse("NULL"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("allCodecs")
    @DisplayName("The Java type is declared as Object, so the registry treats the codec as a fallback rather than rejecting it as a duplicate of the built-in")
    void testJavaTypeIsWideEnoughToBeAFallback(final String name, final TypeCodec<?> codec) {
        // Registering a codec that claims the same exact Java type as a built-in gets it silently dropped.
        // Declaring a supertype is what makes the registry reach these only after the built-in's exact match
        // fails, so this is load-bearing rather than incidental - see each codec's class javadoc.
        final Class<?> rawJavaType = codec.getJavaType().getRawType();
        assertEquals(true, rawJavaType == Object.class || rawJavaType == Number.class,
                () -> name + " declares " + rawJavaType.getName() + ", which the built-in codec already claims");
    }

    static Stream<Arguments> codecCqlTypes() {
        return Stream.of(
                arguments("FlexibleIntCodec", new FlexibleIntCodec(), DataTypes.INT),
                arguments("FlexibleBigIntCodec", new FlexibleBigIntCodec(), DataTypes.BIGINT),
                arguments("FlexibleSmallIntCodec", new FlexibleSmallIntCodec(), DataTypes.SMALLINT),
                arguments("FlexibleTinyIntCodec", new FlexibleTinyIntCodec(), DataTypes.TINYINT),
                arguments("FlexibleDoubleCodec", new FlexibleDoubleCodec(), DataTypes.DOUBLE),
                arguments("FlexibleFloatCodec", new FlexibleFloatCodec(), DataTypes.FLOAT),
                arguments("FlexibleBooleanCodec", new FlexibleBooleanCodec(), DataTypes.BOOLEAN),
                arguments("FlexibleDateCodec", new FlexibleDateCodec(), DataTypes.DATE),
                arguments("FlexibleTimeCodec", new FlexibleTimeCodec(), DataTypes.TIME),
                arguments("FlexibleCounterCodec", new FlexibleCounterCodec(), DataTypes.COUNTER));
    }

    @ParameterizedTest(name = "{0} -> {2}")
    @MethodSource("codecCqlTypes")
    @DisplayName("Each codec binds the CQL type it is named for, which is the key the registry selects it by")
    void testCqlType(final String name, final TypeCodec<?> codec, final DataType expected) {
        assertEquals(expected, codec.getCqlType());
    }

    // ---------------------------------------------------------------------------------------------------
    // The widening contract: an alternative Java type encodes exactly like the canonical one
    // ---------------------------------------------------------------------------------------------------

    static Stream<Arguments> coercions() {
        final ByteBuffer intBytes = TypeCodecs.INT.encode(42, VERSION);
        final ByteBuffer bigIntBytes = TypeCodecs.BIGINT.encode(42L, VERSION);
        final ByteBuffer smallIntBytes = TypeCodecs.SMALLINT.encode((short) 42, VERSION);
        final ByteBuffer wideIntBytes = TypeCodecs.INT.encode(70_000, VERSION);
        final ByteBuffer wideBigIntBytes = TypeCodecs.BIGINT.encode(5_000_000_000L, VERSION);
        final ByteBuffer wideSmallIntBytes = TypeCodecs.SMALLINT.encode((short) 300, VERSION);
        final ByteBuffer tinyIntBytes = TypeCodecs.TINYINT.encode((byte) 42, VERSION);
        final ByteBuffer doubleBytes = TypeCodecs.DOUBLE.encode(42.5d, VERSION);
        final ByteBuffer floatBytes = TypeCodecs.FLOAT.encode(42.5f, VERSION);
        final ByteBuffer trueBytes = TypeCodecs.BOOLEAN.encode(Boolean.TRUE, VERSION);
        final ByteBuffer falseBytes = TypeCodecs.BOOLEAN.encode(Boolean.FALSE, VERSION);
        final ByteBuffer dateBytes = TypeCodecs.DATE.encode(LocalDate.of(2026, 8, 10), VERSION);
        final ByteBuffer timeBytes = TypeCodecs.TIME.encode(LocalTime.of(13, 45, 30), VERSION);
        final ByteBuffer counterBytes = TypeCodecs.BIGINT.encode(42L, VERSION);

        return Stream.of(
                // int - the canonical Integer, plus every other numeric width and a numeric String.
                // The wide cases matter more than they look: a value inside every numeric range cannot tell
                // an int conversion apart from a short or byte one, so each width also gets a value that only
                // fits the column it is bound to.
                arguments("int <- Integer", new FlexibleIntCodec(), 42, intBytes),
                arguments("int <- Long", new FlexibleIntCodec(), 42L, intBytes),
                arguments("int <- Short", new FlexibleIntCodec(), (short) 42, intBytes),
                arguments("int <- Byte", new FlexibleIntCodec(), (byte) 42, intBytes),
                arguments("int <- Double", new FlexibleIntCodec(), 42.0d, intBytes),
                arguments("int <- String", new FlexibleIntCodec(), "42", intBytes),
                arguments("int <- whole-number String with a decimal point and spaces", new FlexibleIntCodec(), " 42.0 ", intBytes),
                arguments("int <- Integer beyond short range", new FlexibleIntCodec(), 70_000, wideIntBytes),
                arguments("int <- Long beyond short range", new FlexibleIntCodec(), 70_000L, wideIntBytes),
                arguments("int <- String beyond short range", new FlexibleIntCodec(), "70000", wideIntBytes),

                // bigint
                arguments("bigint <- Long", new FlexibleBigIntCodec(), 42L, bigIntBytes),
                arguments("bigint <- Integer", new FlexibleBigIntCodec(), 42, bigIntBytes),
                arguments("bigint <- Short", new FlexibleBigIntCodec(), (short) 42, bigIntBytes),
                arguments("bigint <- String", new FlexibleBigIntCodec(), "42", bigIntBytes),
                arguments("bigint <- Long beyond int range", new FlexibleBigIntCodec(), 5_000_000_000L, wideBigIntBytes),
                arguments("bigint <- String beyond int range", new FlexibleBigIntCodec(), "5000000000", wideBigIntBytes),

                // smallint
                arguments("smallint <- Short", new FlexibleSmallIntCodec(), (short) 42, smallIntBytes),
                arguments("smallint <- Integer", new FlexibleSmallIntCodec(), 42, smallIntBytes),
                arguments("smallint <- Long", new FlexibleSmallIntCodec(), 42L, smallIntBytes),
                arguments("smallint <- String", new FlexibleSmallIntCodec(), "42", smallIntBytes),
                arguments("smallint <- Short beyond byte range", new FlexibleSmallIntCodec(), (short) 300, wideSmallIntBytes),
                arguments("smallint <- Integer at its maximum", new FlexibleSmallIntCodec(), 32_767,
                        TypeCodecs.SMALLINT.encode((short) 32_767, VERSION)),
                arguments("smallint <- String beyond byte range", new FlexibleSmallIntCodec(), "300", wideSmallIntBytes),

                // tinyint
                arguments("tinyint <- Byte", new FlexibleTinyIntCodec(), (byte) 42, tinyIntBytes),
                arguments("tinyint <- Integer", new FlexibleTinyIntCodec(), 42, tinyIntBytes),
                arguments("tinyint <- String", new FlexibleTinyIntCodec(), "42", tinyIntBytes),
                arguments("tinyint <- Integer at its minimum", new FlexibleTinyIntCodec(), -128,
                        TypeCodecs.TINYINT.encode((byte) -128, VERSION)),

                // double
                arguments("double <- Double", new FlexibleDoubleCodec(), 42.5d, doubleBytes),
                arguments("double <- Float", new FlexibleDoubleCodec(), 42.5f, doubleBytes),
                arguments("double <- String", new FlexibleDoubleCodec(), "42.5", doubleBytes),
                arguments("double <- Integer", new FlexibleDoubleCodec(), 42,
                        TypeCodecs.DOUBLE.encode(42.0d, VERSION)),

                // float
                arguments("float <- Float", new FlexibleFloatCodec(), 42.5f, floatBytes),
                arguments("float <- Double", new FlexibleFloatCodec(), 42.5d, floatBytes),
                arguments("float <- String", new FlexibleFloatCodec(), "42.5", floatBytes),

                // boolean - the String forms are the point, and they are matched case-insensitively
                arguments("boolean <- Boolean.TRUE", new FlexibleBooleanCodec(), Boolean.TRUE, trueBytes),
                arguments("boolean <- \"true\"", new FlexibleBooleanCodec(), "true", trueBytes),
                arguments("boolean <- \"TRUE\"", new FlexibleBooleanCodec(), "TRUE", trueBytes),
                arguments("boolean <- \"True\"", new FlexibleBooleanCodec(), "True", trueBytes),
                arguments("boolean <- Boolean.FALSE", new FlexibleBooleanCodec(), Boolean.FALSE, falseBytes),
                arguments("boolean <- \"false\"", new FlexibleBooleanCodec(), "false", falseBytes),

                // date - accepted via DataTypeUtils' DATE conversion, which takes several shapes
                arguments("date <- LocalDate", new FlexibleDateCodec(), LocalDate.of(2026, 8, 10), dateBytes),
                arguments("date <- java.sql.Date", new FlexibleDateCodec(),
                        java.sql.Date.valueOf(LocalDate.of(2026, 8, 10)), dateBytes),
                arguments("date <- String yyyy-MM-dd", new FlexibleDateCodec(), "2026-08-10", dateBytes),

                // time
                arguments("time <- java.sql.Time", new FlexibleTimeCodec(),
                        java.sql.Time.valueOf(LocalTime.of(13, 45, 30)), timeBytes),
                arguments("time <- String HH:mm:ss", new FlexibleTimeCodec(), "13:45:30", timeBytes),

                // counter - any Number narrows to the long the counter column holds
                arguments("counter <- Long", new FlexibleCounterCodec(), 42L, counterBytes),
                arguments("counter <- Integer", new FlexibleCounterCodec(), 42, counterBytes),
                arguments("counter <- Short", new FlexibleCounterCodec(), (short) 42, counterBytes));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("coercions")
    @DisplayName("An accepted alternative Java type encodes byte-for-byte like the canonical type")
    void testAlternativeTypeEncodesLikeCanonicalType(final String name, final TypeCodec<?> codec,
                                                     final Object input, final ByteBuffer expected) {
        assertEquals(expected, encode(codec, input));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("coercions")
    @DisplayName("format() applies the same conversion as encode(), so a value logged in a statement matches the value bound")
    void testFormatAppliesTheSameConversionAsEncode(final String name, final TypeCodec<?> codec,
                                                    final Object input, final ByteBuffer expected) {
        // format() and encode() each convert independently in these codecs. Asserting they agree is what
        // catches one of them being changed alone.
        assertEquals(format(codec, canonicalValueOf(codec, input)), format(codec, input));
    }

    // ---------------------------------------------------------------------------------------------------
    // Values the widening does not extend to
    // ---------------------------------------------------------------------------------------------------

    static Stream<Arguments> rejections() {
        return Stream.of(
                arguments("int <- arbitrary Object", new FlexibleIntCodec(), new Object()),
                arguments("bigint <- arbitrary Object", new FlexibleBigIntCodec(), new Object()),
                arguments("double <- arbitrary Object", new FlexibleDoubleCodec(), new Object()),
                arguments("float <- arbitrary Object", new FlexibleFloatCodec(), new Object()),
                arguments("smallint <- arbitrary Object", new FlexibleSmallIntCodec(), new Object()),
                arguments("boolean <- arbitrary Object", new FlexibleBooleanCodec(), new Object()),
                arguments("boolean <- a String that is neither true nor false", new FlexibleBooleanCodec(), "yes"));
    }

    /**
     * Widening is not the same as narrowing: a number too large for the column, or with a fraction it cannot hold,
     * is an error rather than a wrapped or truncated value.
     */
    static Stream<Arguments> numericRejections() {
        return Stream.of(
                arguments("smallint <- Integer too large", new FlexibleSmallIntCodec(), 40_000),
                arguments("tinyint <- Integer too large", new FlexibleTinyIntCodec(), 300),
                arguments("tinyint <- String too large", new FlexibleTinyIntCodec(), "300"),
                arguments("int <- String with a fraction", new FlexibleIntCodec(), "3.7"),
                arguments("bigint <- String too long to be a number", new FlexibleBigIntCodec(), "1".repeat(101)),
                arguments("int <- Long too large", new FlexibleIntCodec(), Long.MAX_VALUE),
                arguments("bigint <- BigInteger too large", new FlexibleBigIntCodec(), new BigInteger("12345678901234567890")),
                arguments("int <- Double with a fraction", new FlexibleIntCodec(), 3.7d),
                arguments("bigint <- Double with a fraction", new FlexibleBigIntCodec(), 2.5d),
                arguments("int <- NaN", new FlexibleIntCodec(), Double.NaN),
                arguments("counter <- Double with a fraction", new FlexibleCounterCodec(), 1.5d),
                arguments("float <- Double too large", new FlexibleFloatCodec(), 1e40d),
                arguments("float <- String too large", new FlexibleFloatCodec(), "1e40"),
                arguments("double <- BigDecimal too large", new FlexibleDoubleCodec(), new BigDecimal("1e400")));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("numericRejections")
    @DisplayName("A number that does not fit the column is rejected with the reason alone, which the caller prefixes with the column, and not the value")
    void testNumberThatDoesNotFitIsRejected(final String name, final TypeCodec<?> codec, final Object input) {
        final RejectedValueException thrown = assertThrows(RejectedValueException.class, () -> encode(codec, input));

        assertTrue(thrown.getMessage().startsWith("the value "), thrown.getMessage());
        assertFalse(thrown.getMessage().contains(String.valueOf(input)), thrown.getMessage());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("rejections")
    @DisplayName("An incompatible value is rejected with an attributable error rather than written as something else")
    void testIncompatibleValueIsRejected(final String name, final TypeCodec<?> codec, final Object input) {
        final IllegalTypeConversionException thrown =
                assertThrows(IllegalTypeConversionException.class, () -> encode(codec, input));

        // The message names the CQL type being bound, which is what makes the failure attributable to a column
        // rather than to "something in the write path".
        assertEquals(true, thrown.getMessage().contains(codec.getCqlType().toString()),
                () -> "expected the CQL type in: " + thrown.getMessage());
    }

    // ---------------------------------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private static ByteBuffer encode(final TypeCodec<?> codec, final Object value) {
        return ((TypeCodec<Object>) codec).encode(value, VERSION);
    }

    @SuppressWarnings("unchecked")
    private static String format(final TypeCodec<?> codec, final Object value) {
        return ((TypeCodec<Object>) codec).format(value);
    }

    /**
     * Decodes what {@code input} encodes to, giving the canonical Java value the column actually holds. Used to
     * check {@code format} against {@code encode} without restating every expected literal a second time.
     */
    private static Object canonicalValueOf(final TypeCodec<?> codec, final Object input) {
        return codec.decode(encode(codec, input), VERSION);
    }
}
