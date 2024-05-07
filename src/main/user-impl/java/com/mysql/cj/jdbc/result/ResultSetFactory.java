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

package com.mysql.cj.jdbc.result;

import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.StatementImpl;
import com.mysql.cj.protocol.ProtocolEntity;
import com.mysql.cj.protocol.ProtocolEntityFactory;
import com.mysql.cj.protocol.ResultsetRows;
import com.mysql.cj.protocol.a.NativePacketPayload;
import com.mysql.cj.protocol.a.result.OkPacket;

import java.sql.SQLException;

public class ResultSetFactory implements ProtocolEntityFactory<ResultSetImpl, NativePacketPayload> {

    private JdbcConnection conn;
    private StatementImpl stmt;

    public ResultSetFactory(JdbcConnection connection, StatementImpl creatorStmt) {
        this.conn = connection;
        this.stmt = creatorStmt;
    }

    @Override
    public ResultSetImpl createFromProtocolEntity(ProtocolEntity protocolEntity) throws CJException {
        try {
            if (protocolEntity instanceof OkPacket) {
                return new ResultSetImpl((OkPacket) protocolEntity, this.conn, this.stmt);

            } else if (protocolEntity instanceof ResultsetRows) {
                return createFromResultsetRows((ResultsetRows) protocolEntity);

            }
            throw ExceptionFactory.createException(WrongArgumentException.class, "Unknown ProtocolEntity class " + protocolEntity);

        } catch (SQLException ex) {
            throw ExceptionFactory.createException(ex.getMessage(), ex);
        }
    }

    /**
     * Build ResultSet from ResultsetRows
     *
     * @param rows
     *            {@link ResultsetRows}
     * @return ResultSetImpl
     * @throws SQLException
     *             if an error occurs
     */
    public ResultSetImpl createFromResultsetRows(ResultsetRows rows) throws SQLException {
        ResultSetImpl rs;

        StatementImpl st = this.stmt;

        if (rows.getOwner() != null) {
            st = ((ResultSetImpl) rows.getOwner()).getOwningStatement();
        }

        rs = new ResultSetImpl(rows, this.conn, st);

        return rs;
    }
}
