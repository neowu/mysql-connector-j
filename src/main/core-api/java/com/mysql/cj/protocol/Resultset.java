/*
 * Copyright (c) 2016, 2024, Oracle and/or its affiliates.
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

package com.mysql.cj.protocol;

/**
 * Represents protocol specific result set,
 * eg., for native protocol, a ProtocolText::Resultset or ProtocolBinary::Resultset entity.
 *
 * See:
 * http://dev.mysql.com/doc/internals/en/com-query-response.html#packet-ProtocolText::Resultset
 * http://dev.mysql.com/doc/internals/en/binary-protocol-resultset.html
 *
 */
public interface Resultset extends ProtocolEntity {

    ColumnDefinition getColumnDefinition();

    /**
     * Does the result set contain rows, or is it the result of a DDL or DML statement?
     *
     * @return true if result set contains rows
     */
    boolean hasRows();

    ResultsetRows getRows();

    /**
     * Set metadata of this Resultset to ResultsetRows it contains.
     */
    void initRowsWithMetadata();

    /**
     * @param nextResultset
     *            Sets the next result set in the result set chain for multiple result sets.
     */
    void setNextResultset(Resultset nextResultset);

    /**
     * Returns the next ResultSet in a multi-resultset "chain", if any,
     * null if none exists.
     *
     * @return the next Resultset
     */
    Resultset getNextResultset();

    /**
     * Returns the update count for this result set (if one exists), otherwise
     * -1.
     *
     * @return return the update count for this result set (if one exists), otherwise
     *         -1.
     */
    long getUpdateCount();

    /**
     * Returns the AUTO_INCREMENT value for the DDL/DML statement which created
     * this result set.
     *
     * @return the AUTO_INCREMENT value for the DDL/DML statement which created
     *         this result set.
     */
    long getUpdateID();

    /**
     * Returns the server informational message returned from a DDL or DML
     * statement (if any), or null if none.
     *
     * @return the server informational message
     */
    String getServerInfo();

}
