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

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Various utility methods for the driver.
 */
public class Util {
    /**
     * Converts a nested exception into a nicer message.
     *
     * @param ex
     *            the exception to expand into a message.
     *
     * @return
     *         a message containing the exception, the message (if any), and a stacktrace.
     */
    public static String stackTraceToString(Throwable ex) {
        StringBuilder traceBuf = new StringBuilder();
        traceBuf.append(Messages.getString("Util.1"));

        if (ex != null) {
            traceBuf.append(ex.getClass().getName());

            String message = ex.getMessage();
            if (message != null) {
                traceBuf.append(Messages.getString("Util.2"));
                traceBuf.append(message);
            }

            StringWriter out = new StringWriter();
            PrintWriter printOut = new PrintWriter(out);

            ex.printStackTrace(printOut);

            traceBuf.append(Messages.getString("Util.3"));
            traceBuf.append(out.toString());
        }
        traceBuf.append(Messages.getString("Util.4"));
        return traceBuf.toString();
    }

    /**
     * Converts long to int, truncating to maximum/minimum value if needed.
     *
     * @param longValue
     *            long value
     * @return int value
     */
    public static int truncateAndConvertToInt(long longValue) {
        return longValue > Integer.MAX_VALUE ? Integer.MAX_VALUE : longValue < Integer.MIN_VALUE ? Integer.MIN_VALUE : (int) longValue;
    }

    /**
     * Converts long[] to int[], truncating to maximum/minimum value if needed.
     *
     * @param longArray
     *            log values
     * @return int values
     */
    public static int[] truncateAndConvertToInt(long[] longArray) {
        int[] intArray = new int[longArray.length];

        for (int i = 0; i < longArray.length; i++) {
            intArray[i] = longArray[i] > Integer.MAX_VALUE ? Integer.MAX_VALUE : longArray[i] < Integer.MIN_VALUE ? Integer.MIN_VALUE : (int) longArray[i];
        }
        return intArray;
    }

}
