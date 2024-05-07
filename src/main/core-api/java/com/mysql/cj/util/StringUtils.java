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
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.WrongArgumentException;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Various utility methods for converting to/from byte arrays in the platform encoding and several other String operations.
 */
public class StringUtils {

    /**
     * Returns the given bytes as a hex and ASCII dump (up to length bytes).
     *
     * @param byteBuffer
     *            the data to dump as hex
     * @param length
     *            the number of bytes to print
     *
     * @return a hex and ASCII dump
     */
    public static String dumpAsHex(byte[] byteBuffer, int length) {
        length = Math.min(length, byteBuffer.length);
        StringBuilder fullOutBuilder = new StringBuilder(length * 4);
        StringBuilder asciiOutBuilder = new StringBuilder(16);

        for (int p = 0, l = 0; p < length; l = 0) { // p: position in buffer (1..length); l: position in line (1..8)
            for (; l < 8 && p < length; p++, l++) {
                int asInt = byteBuffer[p] & 0xff;
                if (asInt < 0x10) {
                    fullOutBuilder.append("0");
                }
                fullOutBuilder.append(Integer.toHexString(asInt)).append(" ");
                asciiOutBuilder.append(" ").append(asInt >= 0x20 && asInt < 0x7f ? (char) asInt : ".");
            }
            for (; l < 8; l++) { // if needed, fill remaining of last line with spaces
                fullOutBuilder.append("   ");
            }
            fullOutBuilder.append("   ").append(asciiOutBuilder).append(System.lineSeparator());
            asciiOutBuilder.setLength(0);
        }
        return fullOutBuilder.toString();
    }

    /**
     * Adds '+' to decimal numbers that are positive (MySQL doesn't understand
     * them otherwise
     *
     * @param dString
     *            The value as a string
     *
     * @return String the string with a '+' added (if needed)
     */
    public static String fixDecimalExponent(String dString) {
        int ePos = dString.indexOf('E');

        if (ePos == -1) {
            ePos = dString.indexOf('e');
        }

        if (ePos != -1) {
            if (dString.length() > ePos + 1) {
                char maybeMinusChar = dString.charAt(ePos + 1);

                if (maybeMinusChar != '-' && maybeMinusChar != '+') {
                    StringBuilder strBuilder = new StringBuilder(dString.length() + 1);
                    strBuilder.append(dString.substring(0, ePos + 1));
                    strBuilder.append('+');
                    strBuilder.append(dString.substring(ePos + 1, dString.length()));
                    dString = strBuilder.toString();
                }
            }
        }

        return dString;
    }

    /**
     * Returns the byte[] representation of the given string using the given encoding.
     *
     * @param s
     *            source string
     * @param encoding
     *            java encoding
     * @return bytes
     */
    public static byte[] getBytes(String s, String encoding) throws WrongArgumentException {
        if (s == null) {
            return new byte[0];
        }
        if (encoding == null) {
            return getBytes(s);
        }
        try {
            return s.getBytes(encoding);
        } catch (UnsupportedEncodingException uee) {
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("StringUtils.0", new Object[] { encoding }), uee);
        }
    }

    /**
     * Returns the byte[] representation of the given string properly wrapped between the given char delimiters using the given encoding.
     *
     * @param s
     *            source string
     * @param beginWrap
     *            opening char delimiter
     * @param endWrap
     *            closing char delimiter
     * @param encoding
     *            java encoding
     * @return bytes
     */
    public static byte[] getBytesWrapped(String s, char beginWrap, char endWrap, String encoding) throws WrongArgumentException {
        byte[] b;

        if (encoding == null) {
            StringBuilder strBuilder = new StringBuilder(s.length() + 2);
            strBuilder.append(beginWrap);
            strBuilder.append(s);
            strBuilder.append(endWrap);

            b = getBytes(strBuilder.toString());
        } else {
            StringBuilder strBuilder = new StringBuilder(s.length() + 2);
            strBuilder.append(beginWrap);
            strBuilder.append(s);
            strBuilder.append(endWrap);

            s = strBuilder.toString();
            b = getBytes(s, encoding);
        }

        return b;
    }

    /**
     * Finds the position of a substring within a string ignoring case.
     *
     * @param searchIn
     *            the string to search in
     * @param searchFor
     *            the array of strings to search for
     * @return the position where <code>searchFor</code> is found within <code>searchIn</code> starting from <code>startingPosition</code>.
     */
    public static int indexOfIgnoreCase(String searchIn, String searchFor) {
        return indexOfIgnoreCase(0, searchIn, searchFor);
    }

    /**
     * Finds the position of a substring within a string ignoring case.
     *
     * @param startingPosition
     *            the position to start the search from
     * @param searchIn
     *            the string to search in
     * @param searchFor
     *            the array of strings to search for
     * @return the position where <code>searchFor</code> is found within <code>searchIn</code> starting from <code>startingPosition</code>.
     */
    public static int indexOfIgnoreCase(int startingPosition, String searchIn, String searchFor) {
        if (searchIn == null || searchFor == null) {
            return -1;
        }

        int searchInLength = searchIn.length();
        int searchForLength = searchFor.length();
        int stopSearchingAt = searchInLength - searchForLength;

        if (startingPosition > stopSearchingAt || searchForLength == 0) {
            return -1;
        }

        // Some locales don't follow upper-case rule, so need to check both
        char firstCharOfSearchForUc = Character.toUpperCase(searchFor.charAt(0));
        char firstCharOfSearchForLc = Character.toLowerCase(searchFor.charAt(0));

        for (int i = startingPosition; i <= stopSearchingAt; i++) {
            if (isCharAtPosNotEqualIgnoreCase(searchIn, i, firstCharOfSearchForUc, firstCharOfSearchForLc)) {
                // find the first occurrence of the first character of searchFor in searchIn
                while (++i <= stopSearchingAt && isCharAtPosNotEqualIgnoreCase(searchIn, i, firstCharOfSearchForUc, firstCharOfSearchForLc)) {
                }
            }

            if (i <= stopSearchingAt && regionMatchesIgnoreCase(searchIn, i, searchFor)) {
                return i;
            }
        }

        return -1;
    }

    /**
     * Finds the position of the first of a consecutive sequence of strings within a string, ignoring case, with the option to skip text delimited by given
     * markers or within comments.
     * <p>
     * Independently of the <code>searchMode</code> provided, when searching for the second and following strings <code>SearchMode.SKIP_WHITE_SPACE</code> will
     * be added and <code>SearchMode.SKIP_BETWEEN_MARKERS</code> removed.
     * </p>
     *
     * @param startingPosition
     *            the position to start the search from
     * @param searchIn
     *            the string to search in
     * @param searchForSequence
     *            searchForSequence
     * @param openingMarkers
     *            characters that delimit the beginning of a text block to skip
     * @param closingMarkers
     *            characters that delimit the end of a text block to skip
     * @param searchMode
     *            a <code>Set</code>, ideally an <code>EnumSet</code>, containing the flags from the enum <code>StringUtils.SearchMode</code> that determine the
     *            behavior of the search
     * @return the position where <code>searchFor</code> is found within <code>searchIn</code> starting from <code>startingPosition</code>.
     */
    public static int indexOfIgnoreCase(int startingPosition, String searchIn, String[] searchForSequence, String openingMarkers, String closingMarkers,
            Set<SearchMode> searchMode) {
        StringInspector strInspector = new StringInspector(searchIn, startingPosition, openingMarkers, closingMarkers, "", searchMode);
        return strInspector.indexOfIgnoreCase(searchForSequence);
    }

    /**
     * Finds the position of the next alphanumeric character within a string, with the option to skip text delimited by given markers or within comments.
     *
     * @param startingPosition
     *            the position to start the search from
     * @param searchIn
     *            the string to search in
     * @param openingMarkers
     *            characters that delimit the beginning of a text block to skip
     * @param closingMarkers
     *            characters that delimit the end of a text block to skip
     * @param overridingMarkers
     *            the subset of <code>openingMarkers</code> that override the remaining markers, e.g., if <code>openingMarkers = "'("</code> and
     *            <code>overridingMarkers = "'"</code> then the block between the outer parenthesis in <code>"start ('max('); end"</code> is strictly consumed,
     *            otherwise the suffix <code>" end"</code> would end up being consumed too in the process of handling the nested parenthesis.
     * @param searchMode
     *            a <code>Set</code>, ideally an <code>EnumSet</code>, containing the flags from the enum <code>StringUtils.SearchMode</code> that determine the
     *            behavior of the search
     * @return the position where the next non-whitespace character is found within <code>searchIn</code> starting from <code>startingPosition</code>.
     */
    public static int indexOfNextAlphanumericChar(int startingPosition, String searchIn, String openingMarkers, String closingMarkers, String overridingMarkers,
            Set<SearchMode> searchMode) {
        StringInspector strInspector = new StringInspector(searchIn, startingPosition, openingMarkers, closingMarkers, overridingMarkers, searchMode);
        return strInspector.indexOfNextAlphanumericChar();
    }

    private static boolean isCharAtPosNotEqualIgnoreCase(String searchIn, int pos, char firstCharOfSearchForUc, char firstCharOfSearchForLc) {
        return Character.toLowerCase(searchIn.charAt(pos)) != firstCharOfSearchForLc && Character.toUpperCase(searchIn.charAt(pos)) != firstCharOfSearchForUc;
    }

    protected static boolean isCharEqualIgnoreCase(char charToCompare, char compareToCharUC, char compareToCharLC) {
        return Character.toLowerCase(charToCompare) == compareToCharLC || Character.toUpperCase(charToCompare) == compareToCharUC;
    }

    /**
     * Splits stringToSplit into a list, using the given delimiter
     *
     * @param stringToSplit
     *            the string to split
     * @param delimiter
     *            the string to split on
     * @param trim
     *            should the split strings be whitespace trimmed?
     *
     * @return the list of strings, split by delimiter
     *
     * @throws IllegalArgumentException
     *             if an error occurs
     */
    public static List<String> split(String stringToSplit, String delimiter, boolean trim) {
        if (stringToSplit == null) {
            return new ArrayList<>();
        }

        if (delimiter == null) {
            throw new IllegalArgumentException();
        }

        String[] tokens = stringToSplit.split(delimiter, -1);
        List<String> tokensList = Arrays.asList(tokens);
        if (trim) {
            tokensList = tokensList.stream().map(String::trim).collect(Collectors.toList());
        }
        return tokensList;
    }

    /**
     * Splits stringToSplit into a list, using the given delimiter and skipping all between the given markers.
     *
     * @param stringToSplit
     *            the string to split
     * @param delimiter
     *            the string to split on
     * @param openingMarkers
     *            characters that delimit the beginning of a text block to skip
     * @param closingMarkers
     *            characters that delimit the end of a text block to skip
     * @param trim
     *            should the split strings be whitespace trimmed?
     *
     * @return the list of strings, split by delimiter
     *
     * @throws IllegalArgumentException
     *             if an error occurs
     */
    public static List<String> split(String stringToSplit, String delimiter, String openingMarkers, String closingMarkers, boolean trim) {
        return split(stringToSplit, delimiter, openingMarkers, closingMarkers, "", trim);
    }

    /**
     * Splits stringToSplit into a list, using the given delimiter and skipping all between the given markers.
     *
     * @param stringToSplit
     *            the string to split
     * @param delimiter
     *            the string to split on
     * @param openingMarkers
     *            characters that delimit the beginning of a text block to skip
     * @param closingMarkers
     *            characters that delimit the end of a text block to skip
     * @param trim
     *            should the split strings be whitespace trimmed?
     * @param searchMode
     *            a <code>Set</code>, ideally an <code>EnumSet</code>, containing the flags from the enum <code>StringUtils.SearchMode</code> that determine the
     *            behaviour of the search
     *
     * @return the list of strings, split by delimiter
     *
     * @throws IllegalArgumentException
     *             if an error occurs
     */
    public static List<String> split(String stringToSplit, String delimiter, String openingMarkers, String closingMarkers, boolean trim,
            Set<SearchMode> searchMode) {
        return split(stringToSplit, delimiter, openingMarkers, closingMarkers, "", trim, searchMode);
    }

    /**
     * Splits stringToSplit into a list, using the given delimiter and skipping all between the given markers.
     *
     * @param stringToSplit
     *            the string to split
     * @param delimiter
     *            the string to split on
     * @param openingMarkers
     *            characters that delimit the beginning of a text block to skip
     * @param closingMarkers
     *            characters that delimit the end of a text block to skip
     * @param overridingMarkers
     *            the subset of <code>openingMarkers</code> that override the remaining markers, e.g., if <code>openingMarkers = "'("</code> and
     *            <code>overridingMarkers = "'"</code> then the block between the outer parenthesis in <code>"start ('max('); end"</code> is strictly consumed,
     *            otherwise the suffix <code>" end"</code> would end up being consumed too in the process of handling the nested parenthesis.
     * @param trim
     *            should the split strings be whitespace trimmed?
     *
     * @return the list of strings, split by delimiter
     *
     * @throws IllegalArgumentException
     *             if an error occurs
     */
    public static List<String> split(String stringToSplit, String delimiter, String openingMarkers, String closingMarkers, String overridingMarkers,
            boolean trim) {
        return split(stringToSplit, delimiter, openingMarkers, closingMarkers, overridingMarkers, trim, SearchMode.__MRK_COM_MYM_HNT_WS);
    }

    /**
     * Splits stringToSplit into a list, using the given delimiter and skipping all between the given markers.
     *
     * @param stringToSplit
     *            the string to split
     * @param delimiter
     *            the string to split on
     * @param openingMarkers
     *            characters that delimit the beginning of a text block to skip
     * @param closingMarkers
     *            characters that delimit the end of a text block to skip
     * @param overridingMarkers
     *            the subset of <code>openingMarkers</code> that override the remaining markers, e.g., if <code>openingMarkers = "'("</code> and
     *            <code>overridingMarkers = "'"</code> then the block between the outer parenthesis in <code>"start ('max('); end"</code> is strictly consumed,
     *            otherwise the suffix <code>" end"</code> would end up being consumed too in the process of handling the nested parenthesis.
     * @param trim
     *            should the split strings be whitespace trimmed?
     * @param searchMode
     *            a <code>Set</code>, ideally an <code>EnumSet</code>, containing the flags from the enum <code>StringUtils.SearchMode</code> that determine the
     *            behaviour of the search
     *
     * @return the list of strings, split by delimiter
     *
     * @throws IllegalArgumentException
     *             if an error occurs
     */
    public static List<String> split(String stringToSplit, String delimiter, String openingMarkers, String closingMarkers, String overridingMarkers,
            boolean trim, Set<SearchMode> searchMode) {
        StringInspector strInspector = new StringInspector(stringToSplit, openingMarkers, closingMarkers, overridingMarkers, searchMode);
        return strInspector.split(delimiter, trim);
    }

    /**
     * Determines whether or not the string 'searchIn' contains the string 'searchFor', disregarding case and starting at 'startAt'. Shorthand for a
     * String.regionMatch(...)
     *
     * @param searchIn
     *            the string to search in
     * @param startAt
     *            the position to start at
     * @param searchFor
     *            the string to search for
     *
     * @return whether searchIn starts with searchFor, ignoring case
     */
    public static boolean regionMatchesIgnoreCase(String searchIn, int startAt, String searchFor) {
        return searchIn.regionMatches(true, startAt, searchFor, 0, searchFor.length());
    }

    /**
     * Determines whether or not the string 'searchIn' starts with the string 'searchFor', dis-regarding case. Shorthand for a String.regionMatch(...)
     *
     * @param searchIn
     *            the string to search in
     * @param searchFor
     *            the string to search for
     *
     * @return whether searchIn starts with searchFor, ignoring case
     */
    public static boolean startsWithIgnoreCase(String searchIn, String searchFor) {
        return regionMatchesIgnoreCase(searchIn, 0, searchFor);
    }

    /**
     * Determines whether or not the string 'searchIn' starts with the string 'searchFor', disregarding case and leading whitespace
     *
     * @param searchIn
     *            the string to search in
     * @param searchFor
     *            the string to search for
     *
     * @return true if the string starts with 'searchFor' ignoring whitespace
     */
    public static boolean startsWithIgnoreCaseAndWs(String searchIn, String searchFor) {
        return startsWithIgnoreCaseAndWs(searchIn, searchFor, 0);
    }

    /**
     * Determines whether or not the string 'searchIn' contains the string 'searchFor', disregarding case and leading whitespace
     *
     * @param searchIn
     *            the string to search in
     * @param searchFor
     *            the string to search for
     * @param beginPos
     *            where to start searching
     *
     * @return true if the string starts with 'searchFor' ignoring whitespace
     */

    public static boolean startsWithIgnoreCaseAndWs(String searchIn, String searchFor, int beginPos) {
        if (searchIn == null) {
            return searchFor == null;
        }

        for (; beginPos < searchIn.length(); beginPos++) {
            if (!Character.isWhitespace(searchIn.charAt(beginPos))) {
                break;
            }
        }

        return regionMatchesIgnoreCase(searchIn, beginPos, searchFor);
    }

    /**
     * Determines whether or not the string 'searchIn' ends with the string 'searchFor', dis-regarding case starting at 'startAt' Shorthand for a
     * String.regionMatch(...)
     *
     * @param searchIn
     *            the string to search in
     * @param searchFor
     *            the string to search for
     *
     * @return whether searchIn ends with searchFor, ignoring case
     */
    public static boolean endsWithIgnoreCase(String searchIn, String searchFor) {
        int len = searchFor.length();
        return searchIn.regionMatches(true, searchIn.length() - len, searchFor, 0, len);
    }

    /**
     * Returns the bytes as an ASCII String.
     *
     * @param buffer
     *            the bytes representing the string
     *
     * @return The ASCII String.
     */
    public static String toAsciiString(byte[] buffer) {
        return toAsciiString(buffer, 0, buffer.length);
    }

    /**
     * Returns the bytes as an ASCII String.
     *
     * @param buffer
     *            the bytes to convert
     * @param startPos
     *            the position to start converting
     * @param length
     *            the length of the string to convert
     *
     * @return the ASCII string
     */
    public static String toAsciiString(byte[] buffer, int startPos, int length) {
        return new String(toAsciiCharArray(buffer, startPos, length));
    }

    /**
     * Returns the bytes as an ASCII String.
     *
     * @param buffer
     *            the bytes to convert
     * @param startPos
     *            the position to start converting
     * @param length
     *            the length of the string to convert
     *
     * @return the ASCII char array
     */
    public static char[] toAsciiCharArray(byte[] buffer, int startPos, int length) {
        char[] charArray = new char[length];
        int readpoint = startPos;

        for (int i = 0; i < length; i++) {
            charArray[i] = (char) buffer[readpoint];
            readpoint++;
        }
        return charArray;
    }

    public static boolean isNullOrEmpty(String str) {
        return str == null || str.isEmpty();
    }

    /**
     * Removes comments and hints from the given string.
     *
     * @param source
     *            the query string to clean up.
     * @param openingMarkers
     *            characters that delimit the beginning of a text block to skip
     * @param closingMarkers
     *            characters that delimit the end of a text block to skip
     * @param allowBackslashEscapes
     *            whether or not backslash escapes are allowed
     * @return the query string with all comment-delimited data removed
     */
    public static String stripCommentsAndHints(final String source, final String openingMarkers, final String closingMarkers,
            final boolean allowBackslashEscapes) {
        StringInspector strInspector = new StringInspector(source, openingMarkers, closingMarkers, "",
                allowBackslashEscapes ? SearchMode.__BSE_MRK_COM_MYM_HNT_WS : SearchMode.__MRK_COM_MYM_HNT_WS);
        return strInspector.stripCommentsAndHints();
    }

    public static boolean isEmptyOrWhitespaceOnly(String str) {
        if (str == null || str.length() == 0) {
            return true;
        }

        int length = str.length();

        for (int i = 0; i < length; i++) {
            if (!Character.isWhitespace(str.charAt(i))) {
                return false;
            }
        }

        return true;
    }

    public static String toString(byte[] value, int offset, int length, String encoding) {
        if (encoding == null || "null".equalsIgnoreCase(encoding)) {
            return new String(value, offset, length);
        }
        try {
            return new String(value, offset, length, encoding);
        } catch (UnsupportedEncodingException uee) {
            throw new Error(Messages.getString("StringUtils.0", new Object[] { encoding }), uee);
        }
    }

    public static String toString(byte[] value, String encoding) {
        if (encoding == null) {
            return new String(value);
        }
        try {
            return new String(value, encoding);
        } catch (UnsupportedEncodingException uee) {
            throw new Error(Messages.getString("StringUtils.0", new Object[] { encoding }), uee);
        }
    }

    public static String toString(byte[] value, Charset charset) {
        return new String(value, charset);
    }

    public static String toString(byte[] value, int offset, int length) {
        return new String(value, offset, length);
    }

    public static String toString(byte[] value) {
        return new String(value);
    }

    public static byte[] getBytes(String value) {
        return value.getBytes();
    }

    public static byte[] getBytes(String value, int offset, int length) {
        return value.substring(offset, offset + length).getBytes();
    }

    public static byte[] getBytes(String value, int offset, int length, String encoding) throws WrongArgumentException {
        if (encoding == null) {
            return getBytes(value, offset, length);
        }

        try {
            return value.substring(offset, offset + length).getBytes(encoding);
        } catch (UnsupportedEncodingException uee) {
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("StringUtils.0", new Object[] { encoding }), uee);
        }
    }

    public static byte[] getBytesNullTerminated(String value, String encoding) {
        Charset cs = Charset.forName(encoding);
        ByteBuffer buf = cs.encode(value);
        int encodedLen = buf.limit();
        byte[] asBytes = new byte[encodedLen + 1];
        buf.get(asBytes, 0, encodedLen);
        asBytes[encodedLen] = 0;

        return asBytes;
    }

    final static char[] EMPTY_SPACE = new char[255];
    static {
        for (int i = 0; i < EMPTY_SPACE.length; i++) {
            EMPTY_SPACE[i] = ' ';
        }
    }

    public static int safeIntParse(String intAsString) {
        try {
            return Integer.parseInt(intAsString);
        } catch (NumberFormatException nfe) {
            return 0;
        }
    }

    public static String safeTrim(String toTrim) {
        return isNullOrEmpty(toTrim) ? toTrim : toTrim.trim();
    }

    /**
     * Constructs a String containing all the elements in the String array bounded and joined by the provided concatenation elements. The last element uses a
     * different delimiter.
     *
     * @param elems
     *            the String array from where to take the elements.
     * @param prefix
     *            the prefix of the resulting String.
     * @param midDelimiter
     *            the delimiter to be used between the N-1 elements
     * @param lastDelimiter
     *            the delimiter to be used before the last element.
     * @param suffix
     *            the suffix of the resulting String.
     * @return
     *         a String built from the provided String array and concatenation elements.
     */
    public static String stringArrayToString(String[] elems, String prefix, String midDelimiter, String lastDelimiter, String suffix) {
        StringBuilder valuesString = new StringBuilder();
        if (elems.length > 1) {
            valuesString.append(Arrays.stream(elems).limit(elems.length - 1).collect(Collectors.joining(midDelimiter, prefix, lastDelimiter)));
        } else {
            valuesString.append(prefix);
        }
        valuesString.append(elems[elems.length - 1]).append(suffix);

        return valuesString.toString();
    }

    public static StringBuilder escapeString(StringBuilder buf, String x, boolean useAnsiQuotedIdentifiers, CharsetEncoder charsetEncoder) {
        int stringLength = x.length();

        buf.append('\'');

        //
        // Note: buf.append(char) is _faster_ than appending in blocks, because the block append requires a System.arraycopy().... go figure...
        //

        for (int i = 0; i < stringLength; ++i) {
            char c = x.charAt(i);

            switch (c) {
                case 0: /* Must be escaped for 'mysql' */
                    buf.append('\\');
                    buf.append('0');
                    break;
                case '\n': /* Must be escaped for logs */
                    buf.append('\\');
                    buf.append('n');
                    break;
                case '\r':
                    buf.append('\\');
                    buf.append('r');
                    break;
                case '\\':
                    buf.append('\\');
                    buf.append('\\');
                    break;
                case '\'':
                    buf.append('\'');
                    buf.append('\'');
                    break;
                case '"': /* Better safe than sorry */
                    if (useAnsiQuotedIdentifiers) {
                        buf.append('\\');
                    }
                    buf.append('"');
                    break;
                case '\032': /* This gives problems on Win32 */
                    buf.append('\\');
                    buf.append('Z');
                    break;
                case '\u00a5':
                case '\u20a9':
                    // escape characters interpreted as backslash by mysql
                    if (charsetEncoder != null) {
                        CharBuffer cbuf = CharBuffer.allocate(1);
                        ByteBuffer bbuf = ByteBuffer.allocate(1);
                        cbuf.put(c);
                        cbuf.position(0);
                        charsetEncoder.encode(cbuf, bbuf, true);
                        if (bbuf.get(0) == '\\') {
                            buf.append('\\');
                        }
                    }
                    buf.append(c);
                    break;

                default:
                    buf.append(c);
            }
        }

        buf.append('\'');

        return buf;
    }

}
