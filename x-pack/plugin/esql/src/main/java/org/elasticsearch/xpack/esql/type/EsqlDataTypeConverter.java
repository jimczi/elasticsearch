/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.type;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.Converter;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DataTypeConverter;
import org.elasticsearch.xpack.esql.core.type.DataTypes;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.AbstractConvertFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToBoolean;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToCartesianPoint;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToCartesianShape;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDatetime;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToGeoPoint;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToGeoShape;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToIP;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToInteger;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToLong;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToString;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToUnsignedLong;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToVersion;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.versionfield.Version;

import java.io.IOException;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.time.ZoneId;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAmount;
import java.util.Locale;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.util.Map.entry;
import static org.elasticsearch.xpack.esql.core.type.DataTypeConverter.safeDoubleToLong;
import static org.elasticsearch.xpack.esql.core.type.DataTypeConverter.safeToInt;
import static org.elasticsearch.xpack.esql.core.type.DataTypeConverter.safeToLong;
import static org.elasticsearch.xpack.esql.core.type.DataTypeConverter.safeToUnsignedLong;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.CARTESIAN_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.CARTESIAN_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.GEO_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.GEO_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.IP;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.NULL;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.TEXT;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.UNSIGNED_LONG;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.VERSION;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.isPrimitive;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.isString;
import static org.elasticsearch.xpack.esql.core.util.NumericUtils.ONE_AS_UNSIGNED_LONG;
import static org.elasticsearch.xpack.esql.core.util.NumericUtils.ZERO_AS_UNSIGNED_LONG;
import static org.elasticsearch.xpack.esql.core.util.NumericUtils.asLongUnsigned;
import static org.elasticsearch.xpack.esql.core.util.NumericUtils.asUnsignedLong;
import static org.elasticsearch.xpack.esql.core.util.NumericUtils.unsignedLongAsNumber;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.UNSPECIFIED;

public class EsqlDataTypeConverter {

    public static final DateFormatter DEFAULT_DATE_TIME_FORMATTER = DateFormatter.forPattern("strict_date_optional_time");

    public static final DateFormatter HOUR_MINUTE_SECOND = DateFormatter.forPattern("strict_hour_minute_second_fraction");

    private static final Map<DataType, BiFunction<Source, Expression, AbstractConvertFunction>> TYPE_TO_CONVERTER_FUNCTION = Map.ofEntries(
        entry(BOOLEAN, ToBoolean::new),
        entry(CARTESIAN_POINT, ToCartesianPoint::new),
        entry(CARTESIAN_SHAPE, ToCartesianShape::new),
        entry(DATETIME, ToDatetime::new),
        // ToDegrees, typeless
        entry(DOUBLE, ToDouble::new),
        entry(GEO_POINT, ToGeoPoint::new),
        entry(GEO_SHAPE, ToGeoShape::new),
        entry(INTEGER, ToInteger::new),
        entry(IP, ToIP::new),
        entry(LONG, ToLong::new),
        // ToRadians, typeless
        entry(KEYWORD, ToString::new),
        entry(TEXT, ToString::new),
        entry(UNSIGNED_LONG, ToUnsignedLong::new),
        entry(VERSION, ToVersion::new)
    );

    /**
     * Returns true if the from type can be converted to the to type, false - otherwise
     */
    public static boolean canConvert(DataType from, DataType to) {
        // Special handling for nulls and if conversion is not requires
        if (from == to || from == NULL) {
            return true;
        }
        // only primitives are supported so far
        return isPrimitive(from) && isPrimitive(to) && converterFor(from, to) != null;
    }

    public static Converter converterFor(DataType from, DataType to) {
        // TODO move EXPRESSION_TO_LONG here if there is no regression
        if (isString(from)) {
            if (to == DataTypes.DATETIME) {
                return EsqlConverter.STRING_TO_DATETIME;
            }
            if (to == DataTypes.IP) {
                return EsqlConverter.STRING_TO_IP;
            }
            if (to == DataTypes.VERSION) {
                return EsqlConverter.STRING_TO_VERSION;
            }
            if (to == DataTypes.DOUBLE) {
                return EsqlConverter.STRING_TO_DOUBLE;
            }
            if (to == DataTypes.LONG) {
                return EsqlConverter.STRING_TO_LONG;
            }
            if (to == DataTypes.INTEGER) {
                return EsqlConverter.STRING_TO_INT;
            }
            if (to == DataTypes.BOOLEAN) {
                return EsqlConverter.STRING_TO_BOOLEAN;
            }
            if (EsqlDataTypes.isSpatial(to)) {
                return EsqlConverter.STRING_TO_SPATIAL;
            }
            if (to == DataTypes.TIME_DURATION) {
                return EsqlConverter.STRING_TO_TIME_DURATION;
            }
            if (to == DataTypes.DATE_PERIOD) {
                return EsqlConverter.STRING_TO_DATE_PERIOD;
            }
        }
        Converter converter = DataTypeConverter.converterFor(from, to);
        if (converter != null) {
            return converter;
        }
        return null;
    }

    public static TemporalAmount parseTemporalAmount(Object val, DataType expectedType) {
        String errorMessage = "Cannot parse [{}] to {}";
        String str = String.valueOf(val);
        if (str == null) {
            return null;
        }
        StringBuilder value = new StringBuilder();
        StringBuilder qualifier = new StringBuilder();
        StringBuilder nextBuffer = value;
        boolean lastWasSpace = false;
        for (char c : str.trim().toCharArray()) {
            if (c == ' ') {
                if (lastWasSpace == false) {
                    nextBuffer = nextBuffer == value ? qualifier : null;
                }
                lastWasSpace = true;
                continue;
            }
            if (nextBuffer == null) {
                throw new ParsingException(Source.EMPTY, errorMessage, val, expectedType);
            }
            nextBuffer.append(c);
            lastWasSpace = false;
        }

        if ((value.isEmpty() || qualifier.isEmpty()) == false) {
            try {
                TemporalAmount result = parseTemporalAmout(Integer.parseInt(value.toString()), qualifier.toString(), Source.EMPTY);
                if (DataTypes.DATE_PERIOD == expectedType && result instanceof Period
                    || DataTypes.TIME_DURATION == expectedType && result instanceof Duration) {
                    return result;
                }
                if (result instanceof Period && expectedType == DataTypes.TIME_DURATION) {
                    errorMessage += ", did you mean " + DataTypes.DATE_PERIOD + "?";
                }
                if (result instanceof Duration && expectedType == DataTypes.DATE_PERIOD) {
                    errorMessage += ", did you mean " + DataTypes.TIME_DURATION + "?";
                }
            } catch (NumberFormatException ex) {
                // wrong pattern
            }
        }

        throw new ParsingException(Source.EMPTY, errorMessage, val, expectedType);
    }

    /**
     * Converts arbitrary object to the desired data type.
     * <p>
     * Throws QlIllegalArgumentException if such conversion is not possible
     */
    public static Object convert(Object value, DataType dataType) {
        DataType detectedType = EsqlDataTypes.fromJava(value);
        if (detectedType == dataType || value == null) {
            return value;
        }
        Converter converter = converterFor(detectedType, dataType);

        if (converter == null) {
            throw new QlIllegalArgumentException(
                "cannot convert from [{}], type [{}] to [{}]",
                value,
                detectedType.typeName(),
                dataType.typeName()
            );
        }

        return converter.convert(value);
    }

    public static DataType commonType(DataType left, DataType right) {
        return DataTypeConverter.commonType(left, right);
    }

    // generally supporting abbreviations from https://en.wikipedia.org/wiki/Unit_of_time
    public static TemporalAmount parseTemporalAmout(Number value, String qualifier, Source source) throws InvalidArgumentException,
        ArithmeticException, ParsingException {
        return switch (qualifier) {
            case "millisecond", "milliseconds", "ms" -> Duration.ofMillis(safeToLong(value));
            case "second", "seconds", "sec", "s" -> Duration.ofSeconds(safeToLong(value));
            case "minute", "minutes", "min" -> Duration.ofMinutes(safeToLong(value));
            case "hour", "hours", "h" -> Duration.ofHours(safeToLong(value));

            case "day", "days", "d" -> Period.ofDays(safeToInt(safeToLong(value)));
            case "week", "weeks", "w" -> Period.ofWeeks(safeToInt(safeToLong(value)));
            case "month", "months", "mo" -> Period.ofMonths(safeToInt(safeToLong(value)));
            case "quarter", "quarters", "q" -> Period.ofMonths(safeToInt(Math.multiplyExact(3L, safeToLong(value))));
            case "year", "years", "yr", "y" -> Period.ofYears(safeToInt(safeToLong(value)));

            default -> throw new ParsingException(source, "Unexpected time interval qualifier: '{}'", qualifier);
        };
    }

    /**
     * The following conversions are used by DateExtract.
     */
    private static ChronoField stringToChrono(Object field) {
        ChronoField chronoField = null;
        try {
            BytesRef br = BytesRefs.toBytesRef(field);
            chronoField = ChronoField.valueOf(br.utf8ToString().toUpperCase(Locale.ROOT));
        } catch (Exception e) {
            return null;
        }
        return chronoField;
    }

    public static long chronoToLong(long dateTime, BytesRef chronoField, ZoneId zone) {
        ChronoField chrono = ChronoField.valueOf(chronoField.utf8ToString().toUpperCase(Locale.ROOT));
        return Instant.ofEpochMilli(dateTime).atZone(zone).getLong(chrono);
    }

    public static long chronoToLong(long dateTime, ChronoField chronoField, ZoneId zone) {
        return Instant.ofEpochMilli(dateTime).atZone(zone).getLong(chronoField);
    }

    /**
     * The following conversions are between String and other data types.
     */
    public static BytesRef stringToIP(BytesRef field) {
        return StringUtils.parseIP(field.utf8ToString());
    }

    public static BytesRef stringToIP(String field) {
        return StringUtils.parseIP(field);
    }

    public static String ipToString(BytesRef field) {
        return DocValueFormat.IP.format(field);
    }

    public static BytesRef stringToVersion(BytesRef field) {
        return new Version(field.utf8ToString()).toBytesRef();
    }

    public static BytesRef stringToVersion(String field) {
        return new Version(field).toBytesRef();
    }

    public static String versionToString(BytesRef field) {
        return new Version(field).toString();
    }

    public static String versionToString(Version field) {
        return field.toString();
    }

    public static String spatialToString(BytesRef field) {
        return UNSPECIFIED.wkbToWkt(field);
    }

    public static BytesRef stringToSpatial(String field) {
        return UNSPECIFIED.wktToWkb(field);
    }

    public static long dateTimeToLong(String dateTime) {
        return DEFAULT_DATE_TIME_FORMATTER.parseMillis(dateTime);
    }

    public static long dateTimeToLong(String dateTime, DateFormatter formatter) {
        return formatter == null ? dateTimeToLong(dateTime) : formatter.parseMillis(dateTime);
    }

    public static String dateTimeToString(long dateTime) {
        return DEFAULT_DATE_TIME_FORMATTER.formatMillis(dateTime);
    }

    public static String dateTimeToString(long dateTime, DateFormatter formatter) {
        return formatter == null ? dateTimeToString(dateTime) : formatter.formatMillis(dateTime);
    }

    public static BytesRef numericBooleanToString(Object field) {
        return new BytesRef(String.valueOf(field));
    }

    public static boolean stringToBoolean(String field) {
        return Boolean.parseBoolean(field);
    }

    public static int stringToInt(String field) {
        try {
            return Integer.parseInt(field);
        } catch (NumberFormatException nfe) {
            try {
                return safeToInt(stringToDouble(field));
            } catch (Exception e) {
                throw new InvalidArgumentException(nfe, "Cannot parse number [{}]", field);
            }
        }
    }

    public static long stringToLong(String field) {
        try {
            return StringUtils.parseLong(field);
        } catch (InvalidArgumentException iae) {
            try {
                return safeDoubleToLong(stringToDouble(field));
            } catch (Exception e) {
                throw new InvalidArgumentException(iae, "Cannot parse number [{}]", field);
            }
        }
    }

    public static double stringToDouble(String field) {
        return StringUtils.parseDouble(field);
    }

    public static BytesRef unsignedLongToString(long number) {
        return new BytesRef(unsignedLongAsNumber(number).toString());
    }

    public static long stringToUnsignedLong(String field) {
        return asLongUnsigned(safeToUnsignedLong(field));
    }

    public static Number stringToIntegral(String field) {
        return StringUtils.parseIntegral(field);
    }

    /**
     * The following conversion are between unsignedLong and other numeric data types.
     */
    public static double unsignedLongToDouble(long number) {
        return NumericUtils.unsignedLongAsNumber(number).doubleValue();
    }

    public static long doubleToUnsignedLong(double number) {
        return NumericUtils.asLongUnsigned(safeToUnsignedLong(number));
    }

    public static int unsignedLongToInt(long number) {
        Number n = NumericUtils.unsignedLongAsNumber(number);
        int i = n.intValue();
        if (i != n.longValue()) {
            throw new InvalidArgumentException("[{}] out of [integer] range", n);
        }
        return i;
    }

    public static long intToUnsignedLong(int number) {
        return longToUnsignedLong(number, false);
    }

    public static long unsignedLongToLong(long number) {
        return DataTypeConverter.safeToLong(unsignedLongAsNumber(number));
    }

    public static long longToUnsignedLong(long number, boolean allowNegative) {
        return allowNegative == false ? NumericUtils.asLongUnsigned(safeToUnsignedLong(number)) : NumericUtils.asLongUnsigned(number);
    }

    public static long bigIntegerToUnsignedLong(BigInteger field) {
        BigInteger unsignedLong = asUnsignedLong(field);
        return NumericUtils.asLongUnsigned(unsignedLong);
    }

    public static BigInteger unsignedLongToBigInteger(long number) {
        return NumericUtils.unsignedLongAsBigInteger(number);
    }

    public static boolean unsignedLongToBoolean(long number) {
        Number n = NumericUtils.unsignedLongAsNumber(number);
        return n instanceof BigInteger || n.longValue() != 0;
    }

    public static long booleanToUnsignedLong(boolean number) {
        return number ? ONE_AS_UNSIGNED_LONG : ZERO_AS_UNSIGNED_LONG;
    }

    public enum EsqlConverter implements Converter {

        STRING_TO_DATE_PERIOD(x -> EsqlDataTypeConverter.parseTemporalAmount(x, DataTypes.DATE_PERIOD)),
        STRING_TO_TIME_DURATION(x -> EsqlDataTypeConverter.parseTemporalAmount(x, DataTypes.TIME_DURATION)),
        STRING_TO_CHRONO_FIELD(EsqlDataTypeConverter::stringToChrono),
        STRING_TO_DATETIME(x -> EsqlDataTypeConverter.dateTimeToLong((String) x)),
        STRING_TO_IP(x -> EsqlDataTypeConverter.stringToIP((String) x)),
        STRING_TO_VERSION(x -> EsqlDataTypeConverter.stringToVersion((String) x)),
        STRING_TO_DOUBLE(x -> EsqlDataTypeConverter.stringToDouble((String) x)),
        STRING_TO_LONG(x -> EsqlDataTypeConverter.stringToLong((String) x)),
        STRING_TO_INT(x -> EsqlDataTypeConverter.stringToInt((String) x)),
        STRING_TO_BOOLEAN(x -> EsqlDataTypeConverter.stringToBoolean((String) x)),
        STRING_TO_SPATIAL(x -> EsqlDataTypeConverter.stringToSpatial((String) x));

        private static final String NAME = "esql-converter";
        private final Function<Object, Object> converter;

        EsqlConverter(Function<Object, Object> converter) {
            this.converter = converter;
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(this);
        }

        public static Converter read(StreamInput in) throws IOException {
            return in.readEnum(EsqlConverter.class);
        }

        @Override
        public Object convert(Object l) {
            if (l == null) {
                return null;
            }
            return converter.apply(l);
        }
    }

    public static BiFunction<Source, Expression, AbstractConvertFunction> converterFunctionFactory(DataType toType) {
        return TYPE_TO_CONVERTER_FUNCTION.get(toType);
    }
}
