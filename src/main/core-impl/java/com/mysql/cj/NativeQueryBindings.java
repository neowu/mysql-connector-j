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
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.exceptions.WrongArgumentException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

public class NativeQueryBindings implements QueryBindings {
    private final Session session;

    /** Bind values for individual fields */
    private final NativeQueryBindValue[] bindValues;

    public NativeQueryBindings(int parameterCount, Session sess) {
        this.session = sess;
        this.bindValues = new NativeQueryBindValue[parameterCount];
        for (int i = 0; i < parameterCount; i++) {
            this.bindValues[i] = new NativeQueryBindValue(this.session);
        }
    }

    private NativeQueryBindings(NativeQueryBindings copy) {
        session = copy.session;
        bindValues = new NativeQueryBindValue[copy.bindValues.length];
        for (int i = 0; i < bindValues.length; i++) {
            bindValues[i] = copy.bindValues[i].copy();
        }
    }

    @Override
    public QueryBindings copy() {
        return new NativeQueryBindings(this);
    }

    @Override
    public BindValue[] getBindValues() {
        return this.bindValues;
    }

    @Override
    public void checkParameterSet(int columnIndex) throws CJException {
        if (!this.bindValues[columnIndex].isSet()) {
            throw ExceptionFactory.createException(Messages.getString("PreparedStatement.40") + (columnIndex + 1),
                    MysqlErrorNumbers.SQL_STATE_WRONG_NO_OF_PARAMETERS, 0, true, null);
        }
    }

    @Override
    public void checkAllParametersSet() throws CJException {
        for (int i = 0; i < this.bindValues.length; i++) {
            checkParameterSet(i);
        }
    }

    @Override
    public void setFromBindValue(int parameterIndex, BindValue bv) {
        BindValue binding = this.bindValues[parameterIndex];
        binding.setBinding(bv.getValue(), bv.getMysqlType());
        binding.setField(bv.getField());
        binding.setScaleOrLength(bv.getScaleOrLength());
    }

    private static final Map<Class<?>, MysqlType> DEFAULT_MYSQL_TYPES = new HashMap<>();
    static {
        DEFAULT_MYSQL_TYPES.put(BigDecimal.class, MysqlType.DECIMAL);
        DEFAULT_MYSQL_TYPES.put(BigInteger.class, MysqlType.BIGINT);
        DEFAULT_MYSQL_TYPES.put(Boolean.class, MysqlType.BOOLEAN);
        DEFAULT_MYSQL_TYPES.put(Byte.class, MysqlType.TINYINT);
        DEFAULT_MYSQL_TYPES.put(Double.class, MysqlType.DOUBLE);
        DEFAULT_MYSQL_TYPES.put(Duration.class, MysqlType.TIME);
        DEFAULT_MYSQL_TYPES.put(Float.class, MysqlType.FLOAT);
        DEFAULT_MYSQL_TYPES.put(Instant.class, MysqlType.TIMESTAMP);
        DEFAULT_MYSQL_TYPES.put(Integer.class, MysqlType.INT);
        DEFAULT_MYSQL_TYPES.put(LocalDate.class, MysqlType.DATE);
        DEFAULT_MYSQL_TYPES.put(LocalDateTime.class, MysqlType.DATETIME); // default JDBC mapping is TIMESTAMP, see B-4
        DEFAULT_MYSQL_TYPES.put(LocalTime.class, MysqlType.TIME);
        DEFAULT_MYSQL_TYPES.put(Long.class, MysqlType.BIGINT);
        DEFAULT_MYSQL_TYPES.put(OffsetDateTime.class, MysqlType.TIMESTAMP); // default JDBC mapping is TIMESTAMP_WITH_TIMEZONE, see B-4
        DEFAULT_MYSQL_TYPES.put(OffsetTime.class, MysqlType.TIME); // default JDBC mapping is TIME_WITH_TIMEZONE, see B-4
        DEFAULT_MYSQL_TYPES.put(Short.class, MysqlType.SMALLINT);
        DEFAULT_MYSQL_TYPES.put(String.class, MysqlType.VARCHAR);
        DEFAULT_MYSQL_TYPES.put(ZonedDateTime.class, MysqlType.TIMESTAMP); // no JDBC mapping is defined
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) {
        if (x == null) {
            setNull(parameterIndex);
            return;
        }
        this.bindValues[parameterIndex].setBinding(x, MysqlType.DECIMAL);
    }

    @Override
    public void setBigInteger(int parameterIndex, BigInteger x) {
        if (x == null) {
            setNull(parameterIndex);
            return;
        }
        this.bindValues[parameterIndex].setBinding(x, MysqlType.BIGINT_UNSIGNED);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) {
        this.bindValues[parameterIndex].setBinding(x, MysqlType.BOOLEAN);
    }

    @Override
    public void setByte(int parameterIndex, byte x) {
        this.bindValues[parameterIndex].setBinding(x, MysqlType.TINYINT);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws WrongArgumentException {
        if (x == Double.POSITIVE_INFINITY || x == Double.NEGATIVE_INFINITY || Double.isNaN(x)) {
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("PreparedStatement.64", new Object[] { x }));
        }
        this.bindValues[parameterIndex].setBinding(x, MysqlType.DOUBLE);
    }

    @Override
    public void setFloat(int parameterIndex, float x) {
        this.bindValues[parameterIndex].setBinding(x, MysqlType.FLOAT);
    }

    @Override
    public void setInt(int parameterIndex, int x) {
        this.bindValues[parameterIndex].setBinding(x, MysqlType.INT);
    }

    @Override
    public void setLong(int parameterIndex, long x) {
        this.bindValues[parameterIndex].setBinding(x, MysqlType.BIGINT);
    }

    @Override
    public void setNull(int parameterIndex) {
        BindValue binding = this.bindValues[parameterIndex];
        binding.setBinding(null, MysqlType.NULL);
    }

    @Override
    public boolean isNull(int parameterIndex) {
        return this.bindValues[parameterIndex].isNull();
    }

    @Override
    public void setShort(int parameterIndex, short x) {
        this.bindValues[parameterIndex].setBinding(x, MysqlType.SMALLINT);
    }

    @Override
    public void setString(int parameterIndex, String x) {
        if (x == null) {
            setNull(parameterIndex);
            return;
        }
        this.bindValues[parameterIndex].setBinding(x, MysqlType.VARCHAR);
    }

    @Override
    public void setObject(int parameterIndex, Object parameterObj) throws CJException {
        if (parameterObj == null) {
            setNull(parameterIndex);
            return;
        }
        MysqlType defaultMysqlType = DEFAULT_MYSQL_TYPES.get(parameterObj.getClass());
        if (defaultMysqlType == null) {
            Optional<MysqlType> mysqlType = DEFAULT_MYSQL_TYPES.entrySet().stream().filter(m -> m.getKey().isAssignableFrom(parameterObj.getClass()))
                    .map(Entry::getValue).findFirst();
            if (mysqlType.isPresent()) {
                defaultMysqlType = mysqlType.get();
            }
        }
        setObject(parameterIndex, parameterObj, defaultMysqlType, -1);
    }

    /**
     * Set the value of a parameter using an object; use the java.lang equivalent objects for integral values.
     *
     * <P>
     * The given Java object will be converted to the targetMysqlType before being sent to the database.
     *
     * @param parameterIndex
     *            the first parameter is 1...
     * @param parameterObj
     *            the object containing the input parameter value
     * @param targetMysqlType
     *            The MysqlType to be send to the database
     * @param scaleOrLength
     *            For Types.DECIMAL or Types.NUMERIC types
     *            this is the number of digits after the decimal. For all other
     *            types this value will be ignored.
     */
    @Override
    public void setObject(int parameterIndex, Object parameterObj, MysqlType targetMysqlType, int scaleOrLength) throws CJException {
        if (parameterObj == null) {
            setNull(parameterIndex);
            return;
        }

        try {
            if (targetMysqlType == null || targetMysqlType == MysqlType.UNKNOWN) {
                throw new SQLFeatureNotSupportedException("mysql type is not supported, type=" + targetMysqlType);
            }

            BindValue binding = this.bindValues[parameterIndex];
            binding.setBinding(parameterObj, targetMysqlType);
            binding.setScaleOrLength(scaleOrLength);

        } catch (Exception ex) {
            throw ExceptionFactory.createException(
                    Messages.getString("PreparedStatement.17") + parameterObj.getClass() + Messages.getString("PreparedStatement.18")
                    + ex.getClass().getName() + Messages.getString("PreparedStatement.19") + ex.getMessage(),
                    ex);
        }
    }

}
