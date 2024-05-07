/*
 * Copyright (c) 2007, 2024, Oracle and/or its affiliates.
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

package com.mysql.cj.jdbc.result;

import com.mysql.cj.protocol.Resultset;
import com.mysql.cj.protocol.ResultsetRowsOwner;

import java.math.BigInteger;
import java.sql.SQLException;

/**
 * This interface is intended to be used by implementors of statement interceptors so that implementors can create static or dynamic (via
 * java.lang.reflect.Proxy) proxy instances of ResultSets. It consists of methods outside of java.sql.Result that are used internally by other classes in the
 * driver.
 *
 * This interface, although public is <strong>not</strong> designed to be consumed publicly other than for the statement interceptor use case.
 */
public interface ResultSetInternalMethods extends java.sql.ResultSet, ResultsetRowsOwner, Resultset {

    /**
     * Closes this ResultSet and releases resources.
     *
     * @param calledExplicitly
     *            was realClose called by the standard ResultSet.close() method, or was it closed internally by the
     *            driver?
     * @throws SQLException
     *             if an error occurs
     */
    void realClose(boolean calledExplicitly) throws SQLException;

    /**
     * Sets the first character of the query that was issued to create
     * this result set. The character should be upper-cased.
     *
     * @param firstCharUpperCase
     *            character
     */
    void setFirstCharOfQuery(char firstCharUpperCase);

    /**
     * Returns the first character of the query that was issued to create this
     * result set, upper-cased.
     *
     * @return character
     */
    char getFirstCharOfQuery();

    void initializeWithMetadata() throws SQLException;

    BigInteger getBigInteger(int columnIndex) throws SQLException;

}
