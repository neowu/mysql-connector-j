/*
 * Copyright (c) 2017, 2024, Oracle and/or its affiliates.
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

package com.mysql.cj;

import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.WrongArgumentException;

import java.math.BigDecimal;
import java.math.BigInteger;

public interface QueryBindings {

    QueryBindings copy();

    BindValue[] getBindValues();

    void checkParameterSet(int columnIndex) throws CJException;

    void checkAllParametersSet() throws CJException;

    void setFromBindValue(int parameterIndex, BindValue bv);

    void setBigDecimal(int parameterIndex, BigDecimal x);

    void setBigInteger(int parameterIndex, BigInteger x);

    void setBoolean(int parameterIndex, boolean x);

    void setByte(int parameterIndex, byte x);

    void setDouble(int parameterIndex, double x) throws WrongArgumentException;

    void setFloat(int parameterIndex, float x);

    void setInt(int parameterIndex, int x);

    void setLong(int parameterIndex, long x);

    void setNull(int parameterIndex);

    boolean isNull(int parameterIndex);

    void setObject(int parameterIndex, Object parameterObj) throws CJException;

    void setObject(int parameterIndex, Object parameterObj, MysqlType targetMysqlType, int scaleOrLength) throws CJException;

    void setShort(int parameterIndex, short x);

    void setString(int parameterIndex, String x);
}
