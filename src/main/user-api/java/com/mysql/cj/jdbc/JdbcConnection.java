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

package com.mysql.cj.jdbc;

import com.mysql.cj.MysqlConnection;

import java.sql.SQLException;

/**
 * This interface contains methods that are considered the "vendor extension" to the JDBC API for MySQL's implementation of java.sql.Connection.
 *
 * For those looking further into the driver implementation, it is not an API that is used for plugability of implementations inside our driver
 * (which is why there are still references to ConnectionImpl throughout the code).
 */
public interface JdbcConnection extends java.sql.Connection, MysqlConnection {
    JdbcPropertySet getPropertySet();

    /**
     * Returns the -session- value of 'auto_increment_increment' from the server if it exists,
     * or '1' if not.
     *
     * @return the -session- value of 'auto_increment_increment'
     */
    int getAutoIncrementIncrement();

    // **************************
    // moved from MysqlJdbcConnection
    // **************************

    /**
     * Clobbers the physical network connection and marks this connection as closed.
     *
     * @throws SQLException
     *             if an error occurs
     */
    void abortInternal() throws SQLException;

    /**
     * Closes connection and frees resources.
     *
     * @param calledExplicitly
     *            is this being called from close()
     * @param issueRollback
     *            should a rollback() be issued?
     * @param skipLocalTeardown
     *            if true, driver tries to close connection normally, performing rollbacks,
     *            closing open statements etc; otherwise the force close is performed
     * @param reason
     *            the exception caused this method call
     * @throws SQLException
     *             if an error occurs
     */
    void realClose(boolean calledExplicitly, boolean issueRollback, boolean skipLocalTeardown, Throwable reason) throws SQLException;

    /**
     * Retrieves this connection object's current database name.
     *
     * @return current database name
     * @throws SQLException
     *             if an error occurs
     */
    String getDatabase() throws SQLException;

}
