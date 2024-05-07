/*
 * Copyright (c) 2002, 2024, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License, version 2.0, as published by
 * the Free Software Foundation.
 *
 * This program is designed to work with certain software that is licensed under separate terms, as designated in a particular file or component or in
 * included license documentation. The authors of MySQL hereby grant you an additional permission to link the program and your derivative works with the
 * separately licensed software that they have either included with the program or referenced in the documentation.
 *
 * Without limiting anything contained in the foregoing, this file, which is part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 */

package com.mysql.cj.util;

import com.mysql.cj.Messages;
import com.mysql.cj.MysqlType;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.InvalidConnectionAttributeException;
import com.mysql.cj.exceptions.WrongArgumentException;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Locale;
import java.util.Properties;
import java.util.TimeZone;
import java.util.regex.Pattern;

/**
 * Time zone conversion routines and other time related methods
 */
public class TimeUtil {
    public static final LocalTime DEFAULT_TIME = LocalTime.of(0, 0);

    public static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    public static final DateTimeFormatter TIME_FORMATTER_NO_FRACT_NO_OFFSET = DateTimeFormatter.ofPattern("HH:mm:ss");
    public static final DateTimeFormatter TIME_FORMATTER_WITH_NANOS_NO_OFFSET = DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSSSSS");
    public static final DateTimeFormatter TIME_FORMATTER_NO_FRACT_WITH_OFFSET = DateTimeFormatter.ofPattern("HH:mm:ssXXX");
    public static final DateTimeFormatter TIME_FORMATTER_WITH_NANOS_WITH_OFFSET = DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSSSSSXXX");
    public static final DateTimeFormatter TIME_FORMATTER_WITH_OPTIONAL_MICROS = new DateTimeFormatterBuilder().appendPattern("HH:mm:ss")
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 6, true).toFormatter();
    public static final DateTimeFormatter DATETIME_FORMATTER_NO_FRACT_NO_OFFSET = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    public static final DateTimeFormatter DATETIME_FORMATTER_WITH_NANOS_NO_OFFSET = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS");
    public static final DateTimeFormatter DATETIME_FORMATTER_NO_FRACT_WITH_OFFSET = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssXXX");
    public static final DateTimeFormatter DATETIME_FORMATTER_WITH_NANOS_WITH_OFFSET = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSSXXX");
    public static final DateTimeFormatter DATETIME_FORMATTER_WITH_OPTIONAL_MICROS = new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd HH:mm:ss")
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 6, true).toFormatter();

    public static final Pattern DATE_LITERAL_WITH_DELIMITERS = Pattern
            .compile("(\\d{4}|\\d{2})[\\p{Punct}&&[^:]](([0])?[1-9]|[1][0-2])[\\p{Punct}&&[^:]](([0])?[1-9]|[1-2]\\d|[3][0-1])");
    public static final Pattern DATE_LITERAL_NO_DELIMITERS = Pattern.compile("(\\d{4}|\\d{2})([0][1-9]|[1][0-2])([0][1-9]|[1-2]\\d|[3][0-1])");

    public static final Pattern TIME_LITERAL_WITH_DELIMITERS = Pattern.compile("(([0-1])?\\d|[2][0-3]):([0-5])?\\d(:([0-5])?\\d(\\.\\d{1,9})?)?");
    public static final Pattern TIME_LITERAL_SHORT6 = Pattern.compile("([0-1]\\d|[2][0-3])([0-5]\\d){2}(\\.\\d{1,9})?");
    public static final Pattern TIME_LITERAL_SHORT4 = Pattern.compile("([0-5]\\d){2}(\\.\\d{1,9})?");
    public static final Pattern TIME_LITERAL_SHORT2 = Pattern.compile("[0-5]\\d(\\.\\d{1,9})?");

    public static final Pattern DATETIME_LITERAL_WITH_DELIMITERS = Pattern.compile(
            "(\\d{4}|\\d{2})\\p{Punct}(([0])?[1-9]|[1][0-2])\\p{Punct}(([0])?[1-9]|[1-2]\\d|[3][0-1])[ T](([0-1])?\\d|[2][0-3])\\p{Punct}([0-5])?\\d(\\p{Punct}([0-5])?\\d(\\.\\d{1,9})?)?");
    public static final Pattern DATETIME_LITERAL_SHORT14 = Pattern
            .compile("\\d{4}([0][1-9]|[1][0-2])([0][1-9]|[1-2]\\d|[3][0-1])([0-1]\\d|[2][0-3])([0-5]\\d){2}(\\.\\d{1,9}){0,1}");
    public static final Pattern DATETIME_LITERAL_SHORT12 = Pattern
            .compile("\\d{2}([0][1-9]|[1][0-2])([0][1-9]|[1-2]\\d|[3][0-1])([0-1]\\d|[2][0-3])([0-5]\\d){2}(\\.\\d{1,9}){0,1}");

    public static final Pattern DURATION_LITERAL_WITH_DAYS = Pattern
            .compile("(-)?(([0-2])?\\d|[3][0-4]) (([0-1])?\\d|[2][0-3])(:([0-5])?\\d(:([0-5])?\\d(\\.\\d{1,9})?)?)?");
    public static final Pattern DURATION_LITERAL_NO_DAYS = Pattern.compile("(-)?\\d{1,3}:([0-5])?\\d(:([0-5])?\\d(\\.\\d{1,9})?)?");

    // Mappings from TimeZone identifications (prefixed by type: Windows, TZ name, MetaZone, TZ alias, ...), to standard TimeZone Ids
    private static final String TIME_ZONE_MAPPINGS_RESOURCE = "/com/mysql/cj/util/TimeZoneMapping.properties";

    private static Properties timeZoneMappings = null;

    /**
     * Returns the 'official' Java timezone name for the given timezone
     *
     * @param timezoneStr
     *            the 'common' timezone name
     *
     * @return the Java timezone name for the given timezone
     */
    public static String getCanonicalTimeZone(String timezoneStr) throws CJException {
        if (timezoneStr == null) {
            return null;
        }

        timezoneStr = timezoneStr.trim();

        // handle '+/-hh:mm' form ...
        if (timezoneStr.length() > 2) {
            if ((timezoneStr.charAt(0) == '+' || timezoneStr.charAt(0) == '-') && Character.isDigit(timezoneStr.charAt(1))) {
                return "GMT" + timezoneStr;
            }
        }

        synchronized (TimeUtil.class) {
            if (timeZoneMappings == null) {
                loadTimeZoneMappings();
            }
        }

        String canonicalTz;
        if ((canonicalTz = timeZoneMappings.getProperty(timezoneStr)) != null) {
            return canonicalTz;
        }

        throw ExceptionFactory.createException(InvalidConnectionAttributeException.class,
                Messages.getString("TimeUtil.UnrecognizedTimeZoneId", new Object[] { timezoneStr }));
    }

    /**
     * Return a new Timestamp object which value is adjusted according to known DATE, DATETIME or TIMESTAMP field precision.
     *
     * @param ts
     *            an original Timestamp object, not modified by this method
     * @param fsp
     *            value in the range from 0 to 6 specifying fractional seconds precision
     * @return A new Timestamp object cloned from the original one and then rounded or truncated according to required fsp value
     */
    public static Timestamp adjustNanosPrecision(Timestamp ts, int fsp) throws WrongArgumentException {
        if (fsp < 0 || fsp > 6) {
            throw ExceptionFactory.createException(WrongArgumentException.class, "fsp value must be in 0 to 6 range.");
        }
        Timestamp res = (Timestamp) ts.clone();
        double tail = Math.pow(10, 9 - fsp);
        int nanos = (int) Math.round(res.getNanos() / tail) * (int) tail;
        if (nanos > 999999999) { // if rounded up to the second then increment seconds
            nanos %= 1000000000; // get last 9 digits
            res.setTime(res.getTime() + 1000); // increment seconds
        }
        res.setNanos(nanos);
        return res;
    }

    /**
     * Return a new LocalDateTime object which value is adjusted according to known DATE, DATETIME or TIMESTAMP field precision.
     *
     * @param x
     *            an original LocalDateTime object, not modified by this method
     * @param fsp
     *            value in the range from 0 to 6 specifying fractional seconds precision
     * @return A new LocalDateTime object cloned from the original one and then rounded or truncated according to required fsp value
     */
    public static LocalDateTime adjustNanosPrecision(LocalDateTime x, int fsp) throws WrongArgumentException {
        if (fsp < 0 || fsp > 6) {
            throw ExceptionFactory.createException(WrongArgumentException.class, "fsp value must be in 0 to 6 range.");
        }
        int originalNano = x.getNano();
        double tail = Math.pow(10, 9 - fsp);

        int adjustedNano = (int) Math.round(originalNano / tail) * (int) tail;
        if (adjustedNano > 999999999) { // if rounded up to the second then increment seconds
            adjustedNano %= 1000000000;
            x = x.plusSeconds(1);
        }
        return x.withNano(adjustedNano);
    }

    public static LocalTime adjustNanosPrecision(LocalTime x, int fsp) throws WrongArgumentException {
        if (fsp < 0 || fsp > 6) {
            throw ExceptionFactory.createException(WrongArgumentException.class, "fsp value must be in 0 to 6 range.");
        }
        int originalNano = x.getNano();
        double tail = Math.pow(10, 9 - fsp);

        int adjustedNano = (int) Math.round(originalNano / tail) * (int) tail;
        if (adjustedNano > 999999999) { // if rounded up to the second then increment seconds
            adjustedNano %= 1000000000;
            x = x.plusSeconds(1);
        }
        return x.withNano(adjustedNano);
    }

    public static Duration adjustNanosPrecision(Duration x, int fsp) throws WrongArgumentException {
        if (fsp < 0 || fsp > 6) {
            throw ExceptionFactory.createException(WrongArgumentException.class, "fsp value must be in 0 to 6 range.");
        }
        int originalNano = x.getNano();
        double tail = Math.pow(10, 9 - fsp);

        int adjustedNano = (int) Math.round(originalNano / tail) * (int) tail;
        if (adjustedNano > 999999999) { // if rounded up to the second then increment seconds
            adjustedNano %= 1000000000;
            x = x.plusSeconds(1);
        }
        return x.withNanos(adjustedNano);
    }

    /**
     * Return a string representation of a fractional seconds part. This method assumes that all Timestamp adjustments are already done before,
     * thus no rounding is needed, only a proper "0" padding to be done.
     *
     * @param nanos
     *            fractional seconds value
     * @param fsp
     *            required fractional part length
     * @return fractional seconds part as a string
     */
    public static String formatNanos(int nanos, int fsp) throws WrongArgumentException {
        return formatNanos(nanos, fsp, true);
    }

    /**
     * Return a string representation of a fractional seconds part. This method assumes that all Timestamp adjustments are already done before,
     * thus no rounding is needed, only a proper "0" padding to be done.
     *
     * @param nanos
     *            fractional seconds value
     * @param fsp
     *            required fractional part length
     * @param truncateTrailingZeros
     *            whether to remove trailing zero characters in a fractional part after formatting
     * @return fractional seconds part as a string
     */
    public static String formatNanos(int nanos, int fsp, boolean truncateTrailingZeros) throws WrongArgumentException {
        if (nanos < 0 || nanos > 999999999) {
            throw ExceptionFactory.createException(WrongArgumentException.class, "nanos value must be in 0 to 999999999 range but was " + nanos);
        }
        if (fsp < 0 || fsp > 6) {
            throw ExceptionFactory.createException(WrongArgumentException.class, "fsp value must be in 0 to 6 range but was " + fsp);
        }

        if (fsp == 0 || nanos == 0) {
            return "0";
        }

        // just truncate because we expect the rounding was done before
        nanos = (int) (nanos / Math.pow(10, 9 - fsp));
        if (nanos == 0) {
            return "0";
        }

        String nanosString = Integer.toString(nanos);
        final String zeroPadding = "000000000";

        nanosString = zeroPadding.substring(0, fsp - nanosString.length()) + nanosString;

        if (truncateTrailingZeros) {
            int pos = fsp - 1; // the end, we're padded to the end by the code above
            while (nanosString.charAt(pos) == '0') {
                pos--;
            }
            nanosString = nanosString.substring(0, pos + 1);
        }
        return nanosString;
    }

    /**
     * Loads a properties file that contains all kinds of time zone mappings.
     */
    private static void loadTimeZoneMappings() throws CJException {
        timeZoneMappings = new Properties();
        try {
            timeZoneMappings.load(TimeUtil.class.getResourceAsStream(TIME_ZONE_MAPPINGS_RESOURCE));
        } catch (IOException e) {
            throw ExceptionFactory.createException(Messages.getString("TimeUtil.LoadTimeZoneMappingError"));
        }
        // bridge all Time Zone ids known by Java
        for (String tz : TimeZone.getAvailableIDs()) {
            if (!timeZoneMappings.containsKey(tz)) {
                timeZoneMappings.put(tz, tz);
            }
        }
    }

    /**
     * Get SimpleDateFormat with a default Calendar which TimeZone is replaced with the provided one.
     * <p>
     * Note: The SimpleDateFormat object returned by this method contains a default Calendar with an altered TimeZone. It's safe to cache it between this method
     * calls because the Calendar object itself is not altered.
     *
     * @param cachedSimpleDateFormat
     *            existing SimpleDateFormat to use instead of creating a new one
     * @param pattern
     *            format pattern
     * @param tz
     *            {@link TimeZone} object replacing the default one
     * @return {@link SimpleDateFormat} object
     */
    public static SimpleDateFormat getSimpleDateFormat(SimpleDateFormat cachedSimpleDateFormat, String pattern, TimeZone tz) {
        SimpleDateFormat sdf = cachedSimpleDateFormat != null && cachedSimpleDateFormat.toPattern().equals(pattern) ? cachedSimpleDateFormat
                : new SimpleDateFormat(pattern, Locale.US);
        if (tz != null) {
            sdf.setTimeZone(tz);
        }
        return sdf;
    }

    public static Object parseToDateTimeObject(String s, MysqlType targetMysqlType) throws WrongArgumentException {
        if (DATE_LITERAL_WITH_DELIMITERS.matcher(s).matches()) {
            return LocalDate.parse(getCanonicalDate(s), DateTimeFormatter.ISO_LOCAL_DATE);

        } else if (DATE_LITERAL_NO_DELIMITERS.matcher(s).matches() && !(targetMysqlType == MysqlType.TIME && TIME_LITERAL_SHORT6.matcher(s).matches())) {
            return s.length() == 8 ? LocalDate.parse(s, DateTimeFormatter.BASIC_ISO_DATE) : LocalDate.parse(s, DateTimeFormatter.ofPattern("yyMMdd"));

        } else if (TIME_LITERAL_WITH_DELIMITERS.matcher(s).matches()) {
            return LocalTime.parse(getCanonicalTime(s),
                    new DateTimeFormatterBuilder().appendPattern("HH:mm:ss").appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).toFormatter());

        } else if (TIME_LITERAL_SHORT6.matcher(s).matches()) {
            return LocalTime.parse(s,
                    new DateTimeFormatterBuilder().appendPattern("HHmmss").appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).toFormatter());

        } else if (TIME_LITERAL_SHORT4.matcher(s).matches()) {
            return LocalTime.parse("00" + s,
                    new DateTimeFormatterBuilder().appendPattern("HHmmss").appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).toFormatter());

        } else if (TIME_LITERAL_SHORT2.matcher(s).matches()) {
            return LocalTime.parse("0000" + s,
                    new DateTimeFormatterBuilder().appendPattern("HHmmss").appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).toFormatter());

        } else if (DATETIME_LITERAL_SHORT14.matcher(s).matches()) {
            return LocalDateTime.parse(s,
                    new DateTimeFormatterBuilder().appendPattern("yyyyMMddHHmmss").appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).toFormatter());

        } else if (DATETIME_LITERAL_SHORT12.matcher(s).matches()) {
            return LocalDateTime.parse(s,
                    new DateTimeFormatterBuilder().appendPattern("yyMMddHHmmss").appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).toFormatter());

        } else if (DATETIME_LITERAL_WITH_DELIMITERS.matcher(s).matches()) {
            return LocalDateTime.parse(getCanonicalDateTime(s),
                    new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd HH:mm:ss").appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).toFormatter());

        } else if (DURATION_LITERAL_WITH_DAYS.matcher(s).matches() || DURATION_LITERAL_NO_DAYS.matcher(s).matches()) {
            s = s.startsWith("-") ? s.replace("-", "-P") : "P" + s;
            s = s.contains(" ") ? s.replace(" ", "DT") : s.replace("P", "PT");
            String[] ch = new String[] { "H", "M", "S" };
            int pos = 0;
            while (s.contains(":")) {
                s = s.replaceFirst(":", ch[pos++]);
            }
            s = s + ch[pos];
            return Duration.parse(s);
        }
        throw ExceptionFactory.createException(WrongArgumentException.class, "There is no known date-time pattern for '" + s + "' value");
    }

    private static String getCanonicalDate(String s) {
        String[] sa = s.split("\\p{Punct}");
        StringBuilder sb = new StringBuilder();
        if (sa[0].length() == 2) {
            sb.append(Integer.parseInt(sa[0]) > 69 ? "19" : "20");
        }
        sb.append(sa[0]);
        sb.append("-");
        if (sa[1].length() == 1) {
            sb.append("0");
        }
        sb.append(sa[1]);
        sb.append("-");
        if (sa[2].length() == 1) {
            sb.append("0");
        }
        sb.append(sa[2]);

        return sb.toString();
    }

    private static String getCanonicalTime(String s) {
        String[] sa = s.split("\\p{Punct}");
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < sa.length; i++) {
            if (i > 0) {
                sb.append(i < 3 ? ":" : ".");
            }
            if (i < 3 && sa[i].length() == 1) {
                sb.append("0");
            }
            sb.append(sa[i]);

        }
        if (sa.length < 3) {
            sb.append(":00");
        }

        return sb.toString();
    }

    private static String getCanonicalDateTime(String s) {
        String[] sa = s.split("[ T]");
        return getCanonicalDate(sa[0]) + " " + getCanonicalTime(sa[1]);
    }

    public static String getDurationString(Duration x) {
        String s = (x.isNegative() ? "-" + x.abs().toString() : x.toString()).replace("PT", "");
        if (s.contains("M")) {
            s = s.replace("H", ":");
            if (s.contains("S")) {
                s = s.replace("M", ":").replace("S", "");
            } else {
                s = s.replace("M", ":0");
            }
        } else {
            s = s.replace("H", ":0:0");
        }
        return s;
    }

}
