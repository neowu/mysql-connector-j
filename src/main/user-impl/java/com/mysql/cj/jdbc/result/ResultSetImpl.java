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

package com.mysql.cj.jdbc.result;

import com.mysql.cj.Messages;
import com.mysql.cj.NativeSession;
import com.mysql.cj.Session;
import com.mysql.cj.WarningListener;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.StatementImpl;
import com.mysql.cj.jdbc.exceptions.NotUpdatable;
import com.mysql.cj.jdbc.exceptions.SQLError;
import com.mysql.cj.jdbc.exceptions.SQLExceptionsMapping;
import com.mysql.cj.protocol.ColumnDefinition;
import com.mysql.cj.protocol.ResultsetRows;
import com.mysql.cj.protocol.a.result.NativeResultset;
import com.mysql.cj.protocol.a.result.OkPacket;
import com.mysql.cj.result.BigDecimalValueFactory;
import com.mysql.cj.result.BooleanValueFactory;
import com.mysql.cj.result.ByteValueFactory;
import com.mysql.cj.result.DoubleValueFactory;
import com.mysql.cj.result.DurationValueFactory;
import com.mysql.cj.result.Field;
import com.mysql.cj.result.FloatValueFactory;
import com.mysql.cj.result.IntegerValueFactory;
import com.mysql.cj.result.LocalDateTimeValueFactory;
import com.mysql.cj.result.LocalDateValueFactory;
import com.mysql.cj.result.LocalTimeValueFactory;
import com.mysql.cj.result.LongValueFactory;
import com.mysql.cj.result.OffsetDateTimeValueFactory;
import com.mysql.cj.result.OffsetTimeValueFactory;
import com.mysql.cj.result.ShortValueFactory;
import com.mysql.cj.result.StringValueFactory;
import com.mysql.cj.result.ValueFactory;
import com.mysql.cj.result.ZonedDateTimeValueFactory;
import com.mysql.cj.util.StringUtils;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.sql.Array;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLType;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.TimeZone;

public class ResultSetImpl extends NativeResultset implements ResultSetInternalMethods, WarningListener {
    /** The Connection instance that created us */
    protected volatile JdbcConnection connection;

    protected NativeSession session = null;

    /**
     * First character of the query that created this result set...Used to determine whether or not to parse server info messages in certain
     * circumstances.
     */
    protected char firstCharOfQuery;

    /** Has this result set been closed? */
    protected boolean isClosed = false;

    /** The statement that created us */
    private com.mysql.cj.jdbc.StatementImpl owningStatement;

    /** The warning chain */
    protected java.sql.SQLWarning warningChain = null;

    private ValueFactory<Boolean> booleanValueFactory;
    private ValueFactory<Byte> byteValueFactory;
    private ValueFactory<Short> shortValueFactory;
    private ValueFactory<Integer> integerValueFactory;
    private ValueFactory<Long> longValueFactory;
    private ValueFactory<Float> floatValueFactory;
    private ValueFactory<Double> doubleValueFactory;
    private ValueFactory<BigDecimal> bigDecimalValueFactory;

    private ValueFactory<LocalDate> defaultLocalDateValueFactory;
    private ValueFactory<LocalDateTime> defaultLocalDateTimeValueFactory;
    private ValueFactory<LocalTime> defaultLocalTimeValueFactory;

    private ValueFactory<OffsetTime> defaultOffsetTimeValueFactory;
    private ValueFactory<OffsetDateTime> defaultOffsetDateTimeValueFactory;
    private ValueFactory<ZonedDateTime> defaultZonedDateTimeValueFactory;

    protected boolean treatMysqlDatetimeAsTimestamp = false;
    protected boolean yearIsDateType = true;

    /**
     * Create a result set for an executeUpdate statement.
     *
     * @param ok
     *            {@link OkPacket}
     * @param conn
     *            the Connection that created us.
     * @param creatorStmt
     *            the Statement that created us.
     */
    public ResultSetImpl(OkPacket ok, JdbcConnection conn, StatementImpl creatorStmt) {
        super(ok);

        this.connection = conn;
        this.owningStatement = creatorStmt;

        if (this.connection != null) {
            this.session = (NativeSession) conn.getSession();
        }
    }

    /**
     * Creates a new ResultSet object.
     *
     * @param tuples
     *            actual row data
     * @param conn
     *            the Connection that created us.
     * @param creatorStmt
     *            the Statement that created us.
     *
     * @throws SQLException
     *             if an error occurs
     */
    public ResultSetImpl(ResultsetRows tuples, JdbcConnection conn, StatementImpl creatorStmt) throws SQLException {
        this.connection = conn;
        this.session = (NativeSession) conn.getSession();

        this.owningStatement = creatorStmt;

        PropertySet pset = this.connection.getPropertySet();
        this.treatMysqlDatetimeAsTimestamp = pset.getBooleanProperty(PropertyKey.treatMysqlDatetimeAsTimestamp).getValue();
        this.yearIsDateType = pset.getBooleanProperty(PropertyKey.yearIsDateType).getValue();

        this.booleanValueFactory = new BooleanValueFactory(pset);
        this.byteValueFactory = new ByteValueFactory(pset);
        this.shortValueFactory = new ShortValueFactory(pset);
        this.integerValueFactory = new IntegerValueFactory(pset);
        this.longValueFactory = new LongValueFactory(pset);
        this.floatValueFactory = new FloatValueFactory(pset);
        this.doubleValueFactory = new DoubleValueFactory(pset);
        this.bigDecimalValueFactory = new BigDecimalValueFactory(pset);

        this.defaultLocalDateValueFactory = new LocalDateValueFactory(pset, this);
        this.defaultLocalTimeValueFactory = new LocalTimeValueFactory(pset, this);
        this.defaultLocalDateTimeValueFactory = new LocalDateTimeValueFactory(pset);

        TimeZone defaultTimeZone = this.session.getProtocol().getServerSession().getDefaultTimeZone();
        this.defaultOffsetTimeValueFactory = new OffsetTimeValueFactory(pset, defaultTimeZone);

        try {
            this.defaultOffsetDateTimeValueFactory = new OffsetDateTimeValueFactory(pset, defaultTimeZone,
                    this.session.getProtocol().getServerSession().getSessionTimeZone());
            this.defaultZonedDateTimeValueFactory = new ZonedDateTimeValueFactory(pset, defaultTimeZone,
                    this.session.getProtocol().getServerSession().getSessionTimeZone());
        } catch (CJException e) {
            throw SQLExceptionsMapping.translateException(e);
        }
        this.columnDefinition = tuples.getMetadata();
        this.rowData = tuples;
        this.updateCount = this.rowData.size();

        // Check for no results
        if (this.rowData.size() > 0) {
            if (this.updateCount == 1) {
                if (this.thisRow == null) {
                    this.updateCount = -1;
                }
            }
        } else {
            this.thisRow = null;
        }

        this.rowData.setOwner(this);

        if (this.columnDefinition.getFields() != null) {
            initializeWithMetadata();
        } // else called by Connection.initializeResultsMetadataFromCache() when cached

        setRowPositionValidity();
    }

    @Override
    public void initializeWithMetadata() {
            initRowsWithMetadata();
    }

    @Override
    public boolean absolute(int row) throws SQLException {

            if (!hasRows()) {
                throw SQLError.createSQLException(Messages.getString("ResultSet.ResultSet_is_from_UPDATE._No_Data_115"),
                        MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR);
            }

            if (isStrictlyForwardOnly()) {
                throw SQLExceptionsMapping.translateException(ExceptionFactory.createException(Messages.getString("ResultSet.ForwardOnly")));
            }

            boolean b;

            if (this.rowData.size() == 0) {
                b = false;
            } else if (row == 0) {
                beforeFirst();
                b = false;
            } else if (row == 1) {
                b = first();
            } else if (row == -1) {
                b = last();
            } else if (row > this.rowData.size()) {
                afterLast();
                b = false;
            } else if (row < 0) {
                // adjust to reflect after end of result set
                int newRowPosition = this.rowData.size() + row + 1;

                if (newRowPosition <= 0) {
                    beforeFirst();
                    b = false;
                } else {
                    b = absolute(newRowPosition);
                }
            } else {
                row--; // adjust for index difference
                this.rowData.setCurrentRow(row);
                this.thisRow = this.rowData.get(row);
                b = true;
            }

            setRowPositionValidity();

            return b;

    }

    @Override
    public void afterLast() throws SQLException {
        try {
            if (!hasRows()) {
                throw SQLError.createSQLException(Messages.getString("ResultSet.ResultSet_is_from_UPDATE._No_Data_115"),
                        MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR);
            }
            if (isStrictlyForwardOnly()) {
                throw ExceptionFactory.createException(Messages.getString("ResultSet.ForwardOnly"));
            }
            if (this.rowData.size() != 0) {
                this.rowData.afterLast();
                this.thisRow = null;
            }
            setRowPositionValidity();
        } catch (CJException e) {
            throw SQLExceptionsMapping.translateException(e);
        }
    }

    @Override
    public void beforeFirst() throws SQLException {
        try {
            if (!hasRows()) {
                throw SQLError.createSQLException(Messages.getString("ResultSet.ResultSet_is_from_UPDATE._No_Data_115"),
                        MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR);
            }

            if (isStrictlyForwardOnly()) {
                throw ExceptionFactory.createException(Messages.getString("ResultSet.ForwardOnly"));
            }

            if (this.rowData.size() == 0) {
                return;
            }

            this.rowData.beforeFirst();
            this.thisRow = null;

            setRowPositionValidity();
        } catch (CJException e) {
            throw SQLExceptionsMapping.translateException(e);
        }
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    /**
     * Ensures that the result set is not closed
     *
     * @return connection
     *
     * @throws SQLException
     *             if the result set is closed
     */
    protected final JdbcConnection checkClosed() throws SQLException {
        JdbcConnection c = this.connection;

        if (c == null) {
            throw SQLError.createSQLException(Messages.getString("ResultSet.Operation_not_allowed_after_ResultSet_closed_144"),
                    MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR);
        }

        return c;
    }

    /**
     * Checks if columnIndex is within the number of columns in this result set.
     *
     * @param columnIndex
     *            the index to check
     *
     * @throws SQLException
     *             if the index is out of bounds
     */
    protected final void checkColumnBounds(int columnIndex) throws SQLException {
        if (columnIndex < 1) {
            throw SQLError.createSQLException(
                    Messages.getString("ResultSet.Column_Index_out_of_range_low",
                            new Object[]{Integer.valueOf(columnIndex), Integer.valueOf(this.columnDefinition.getFields().length)}),
                    MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT);
        } else if (columnIndex > this.columnDefinition.getFields().length) {
            throw SQLError.createSQLException(
                    Messages.getString("ResultSet.Column_Index_out_of_range_high",
                            new Object[]{Integer.valueOf(columnIndex), Integer.valueOf(this.columnDefinition.getFields().length)}),
                    MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT);
        }
    }

    /**
     * Ensures that the cursor is positioned on a valid row and that the result
     * set is not closed
     *
     * @throws SQLException
     *             if the result set is not in a valid state for traversal
     */
    protected void checkRowPos() throws SQLException {
        checkClosed();

        if (!this.onValidRow) {
            throw SQLError.createSQLException(Messages.getString(this.invalidRowReasonMessageKey), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR);
        }
    }

    private boolean onValidRow = false;
    private String invalidRowReasonMessageKey = null;

    private void setRowPositionValidity() {
        if (!this.rowData.isDynamic() && this.rowData.isEmpty()) {
            this.invalidRowReasonMessageKey = "ResultSet.Illegal_operation_on_empty_result_set";
            this.onValidRow = false;
        } else if (this.rowData.isBeforeFirst()) {
            this.invalidRowReasonMessageKey = "ResultSet.Before_start_of_result_set_146";
            this.onValidRow = false;
        } else if (this.rowData.isAfterLast()) {
            this.invalidRowReasonMessageKey = "ResultSet.After_end_of_result_set_148";
            this.onValidRow = false;
        } else {
            this.onValidRow = true;
            this.invalidRowReasonMessageKey = null;
        }
    }

    @Override
    public void clearWarnings() throws SQLException {
        this.warningChain = null;
    }

    @Override
    public void close() throws SQLException {
        realClose(true);
    }

    @Override
    public void deleteRow() throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public int findColumn(String columnName) throws SQLException {
        int index = this.columnDefinition.findColumn(columnName, 1);

        if (index == -1) {
            throw SQLError.createSQLException(
                    Messages.getString("ResultSet.Column____112") + columnName + Messages.getString("ResultSet.___not_found._113"),
                    MysqlErrorNumbers.SQL_STATE_COLUMN_NOT_FOUND);
        }

        return index;
    }

    @Override
    public boolean first() throws SQLException {
        if (!hasRows()) {
            throw SQLError.createSQLException(Messages.getString("ResultSet.ResultSet_is_from_UPDATE._No_Data_115"),
                    MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR);
        }

        if (isStrictlyForwardOnly()) {
            throw SQLExceptionsMapping.translateException(ExceptionFactory.createException(Messages.getString("ResultSet.ForwardOnly")));
        }

        boolean b = true;

        if (this.rowData.isEmpty()) {
            b = false;
        } else {
            this.rowData.beforeFirst();
            this.thisRow = this.rowData.next();
        }

        setRowPositionValidity();

        return b;
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public Array getArray(String colName) throws SQLException {
        return getArray(findColumn(colName));
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);
        return getBinaryStream(columnIndex);
    }

    @Override
    public InputStream getAsciiStream(String columnName) throws SQLException {
        return getAsciiStream(findColumn(columnName));
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);
        return this.thisRow.getValue(columnIndex - 1, this.bigDecimalValueFactory);
    }

    @Deprecated
    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);
        ValueFactory<BigDecimal> vf = new BigDecimalValueFactory(this.session.getPropertySet(), scale);
        vf.setPropertySet(this.connection.getPropertySet());
        return this.thisRow.getValue(columnIndex - 1, vf);
    }

    @Override
    public BigDecimal getBigDecimal(String columnName) throws SQLException {
        return getBigDecimal(findColumn(columnName));
    }

    @Deprecated
    @Override
    public BigDecimal getBigDecimal(String columnName, int scale) throws SQLException {
        return getBigDecimal(findColumn(columnName), scale);
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public InputStream getBinaryStream(String columnName) throws SQLException {
        return getBinaryStream(findColumn(columnName));
    }

    @Override
    public java.sql.Blob getBlob(int columnIndex) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public java.sql.Blob getBlob(String colName) throws SQLException {
        return getBlob(findColumn(colName));
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        Boolean res = getObject(columnIndex, Boolean.TYPE);
        return res == null ? false : res;
    }

    @Override
    public boolean getBoolean(String columnName) throws SQLException {
        return getBoolean(findColumn(columnName));
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        Byte res = getObject(columnIndex, Byte.TYPE);
        return res == null ? (byte) 0 : res;
    }

    @Override
    public byte getByte(String columnName) throws SQLException {
        return getByte(findColumn(columnName));
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);
        return this.thisRow.getBytes(columnIndex - 1);
    }

    @Override
    public byte[] getBytes(String columnName) throws SQLException {
        return getBytes(findColumn(columnName));
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public Reader getCharacterStream(String columnName) throws SQLException {
        return getCharacterStream(findColumn(columnName));
    }

    @Override
    public java.sql.Clob getClob(int columnIndex) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public java.sql.Clob getClob(String colName) throws SQLException {
        return getClob(findColumn(colName));
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public Date getDate(String columnName) throws SQLException {
        return getDate(findColumn(columnName));
    }

    @Override
    public Date getDate(String columnName, Calendar cal) throws SQLException {
        return getDate(findColumn(columnName), cal);
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        Double res = getObject(columnIndex, Double.TYPE);
        return res == null ? (double) 0 : res;
    }

    @Override
    public double getDouble(String columnName) throws SQLException {
        return getDouble(findColumn(columnName));
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        Float res = getObject(columnIndex, Float.TYPE);
        return res == null ? (float) 0 : res;
    }

    @Override
    public float getFloat(String columnName) throws SQLException {
        return getFloat(findColumn(columnName));
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        Integer res = getObject(columnIndex, Integer.TYPE);
        return res == null ? 0 : res;
    }

    @Override
    public BigInteger getBigInteger(int columnIndex) throws SQLException {
        String stringVal = getString(columnIndex);
        if (stringVal == null) {
            return null;
        }
        try {
            return new BigInteger(stringVal);
        } catch (NumberFormatException nfe) {
            throw SQLError.createSQLException(
                    Messages.getString("ResultSet.Bad_format_for_BigInteger", new Object[] { Integer.valueOf(columnIndex), stringVal }),
                    MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT);
        }
    }

    @Override
    public int getInt(String columnName) throws SQLException {
        return getInt(findColumn(columnName));
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        Long res = getObject(columnIndex, Long.TYPE);
        return res == null ? 0L : res;
    }

    @Override
    public long getLong(String columnName) throws SQLException {
        return getLong(findColumn(columnName));
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        Short res = getObject(columnIndex, Short.TYPE);
        return res == null ? (short) 0 : res;
    }

    @Override
    public short getShort(String columnName) throws SQLException {
        return getShort(findColumn(columnName));
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);
        ValueFactory<String> vf = new StringValueFactory(this.session.getPropertySet());
        return this.thisRow.getValue(columnIndex - 1, vf);
    }

    @Override
    public String getString(String columnName) throws SQLException {
        return getString(findColumn(columnName));
    }

    private String getStringForClob(int columnIndex) throws SQLException {
        String asString = null;

        String forcedEncoding = this.connection.getPropertySet().getStringProperty(PropertyKey.clobCharacterEncoding).getStringValue();

        if (forcedEncoding == null) {
            asString = getString(columnIndex);
        } else {
            byte[] asBytes = getBytes(columnIndex);

            if (asBytes != null) {
                asString = StringUtils.toString(asBytes, forcedEncoding);
            }
        }

        return asString;
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public Time getTime(String columnName) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public Time getTime(String columnName, Calendar cal) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    public LocalDate getLocalDate(int columnIndex) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);
        return this.thisRow.getValue(columnIndex - 1, this.defaultLocalDateValueFactory);
    }

    public LocalDateTime getLocalDateTime(int columnIndex) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);
        return this.thisRow.getValue(columnIndex - 1, this.defaultLocalDateTimeValueFactory);
    }

    public LocalTime getLocalTime(int columnIndex) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);
        return this.thisRow.getValue(columnIndex - 1, this.defaultLocalTimeValueFactory);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public Timestamp getTimestamp(String columnName) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public Timestamp getTimestamp(String columnName, Calendar cal) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public Reader getNCharacterStream(String columnName) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public NClob getNClob(String columnName) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);

        String fieldEncoding = this.columnDefinition.getFields()[columnIndex - 1].getEncoding();
        if (fieldEncoding == null || !fieldEncoding.equals("UTF-8")) {
            throw new SQLException("Can not call getNString() when field's charset isn't UTF-8");
        }
        return getString(columnIndex);
    }

    @Override
    public String getNString(String columnName) throws SQLException {
        return getNString(findColumn(columnName));
    }

    @Override
    public int getConcurrency() {
        return CONCUR_READ_ONLY;
    }

    @Override
    public String getCursorName() throws SQLException {
        throw SQLError.createSQLException(Messages.getString("ResultSet.Positioned_Update_not_supported"), MysqlErrorNumbers.SQL_STATE_DRIVER_NOT_CAPABLE);
    }

    @Override
    public int getFetchDirection() {
        return FETCH_FORWARD;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public char getFirstCharOfQuery() {
            return this.firstCharOfQuery;
    }

    @Override
    public java.sql.ResultSetMetaData getMetaData() throws SQLException {
        checkClosed();

        return new ResultSetMetaData(this.session, this.columnDefinition.getFields(), this.yearIsDateType);
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);

        int columnIndexMinusOne = columnIndex - 1;

        // we can't completely rely on code below because primitives have default values for null (e.g. int->0)
        if (this.thisRow.getNull(columnIndexMinusOne)) {
            return null;
        }

        Field field = this.columnDefinition.getFields()[columnIndexMinusOne];
        switch (field.getMysqlType()) {
            case BIT:
                // TODO Field sets binary and blob flags if the length of BIT field is > 1; is it needed at all?
                if (field.isBinary() || field.isBlob()) {
                    byte[] data = getBytes(columnIndex);
                    return data;
                }

                return field.isSingleBit() ? Boolean.valueOf(getBoolean(columnIndex)) : getBytes(columnIndex);

            case BOOLEAN:
                return Boolean.valueOf(getBoolean(columnIndex));

            case TINYINT:
                return Integer.valueOf(getByte(columnIndex));

            case TINYINT_UNSIGNED:
            case SMALLINT:
            case SMALLINT_UNSIGNED:
            case MEDIUMINT:
            case MEDIUMINT_UNSIGNED:
            case INT:
                return Integer.valueOf(getInt(columnIndex));

            case INT_UNSIGNED:
            case BIGINT:
                return Long.valueOf(getLong(columnIndex));

            case BIGINT_UNSIGNED:
                return getBigInteger(columnIndex);

            case DECIMAL:
            case DECIMAL_UNSIGNED:
                String stringVal = getString(columnIndex);

                if (stringVal != null) {
                    if (stringVal.length() == 0) {
                        return new BigDecimal(0);
                    }

                    try {
                        return new BigDecimal(stringVal);
                    } catch (NumberFormatException ex) {
                        throw SQLError.createSQLException(
                                Messages.getString("ResultSet.Bad_format_for_BigDecimal", new Object[] { stringVal, Integer.valueOf(columnIndex) }),
                                MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT);
                    }
                }
                return null;

            case FLOAT:
            case FLOAT_UNSIGNED:
                return getFloat(columnIndex);

            case DOUBLE:
            case DOUBLE_UNSIGNED:
                return getDouble(columnIndex);

            case CHAR:
            case ENUM:
            case SET:
            case VARCHAR:
            case TINYTEXT:
                return getString(columnIndex);

            case TEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
            case JSON:
                return getStringForClob(columnIndex);
            case GEOMETRY:
                return getBytes(columnIndex);

            case BINARY:
            case VARBINARY:
            case TINYBLOB:
            case MEDIUMBLOB:
            case LONGBLOB:
            case BLOB:
                return getBytes(columnIndex);

            case YEAR:
                return this.yearIsDateType ? getDate(columnIndex) : Short.valueOf(getShort(columnIndex));

            case DATE:
                return getDate(columnIndex);

            case TIME:
                return getTime(columnIndex);

            case TIMESTAMP:
                return getTimestamp(columnIndex);

            case DATETIME:
                return this.treatMysqlDatetimeAsTimestamp ? getTimestamp(columnIndex) : getLocalDateTime(columnIndex);

            default:
                return getString(columnIndex);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        if (type == null) {
            throw SQLError.createSQLException("Type parameter can not be null", MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT);
        }

        if (type.equals(String.class)) {
            return (T) getString(columnIndex);

        } else if (type.equals(Integer.class) || type.equals(Integer.TYPE)) {
            checkRowPos();
            checkColumnBounds(columnIndex);
            return (T) this.thisRow.getValue(columnIndex - 1, this.integerValueFactory);

        } else if (type.equals(LocalDate.class)) {
            return (T) getLocalDate(columnIndex);

        } else if (type.equals(LocalDateTime.class)) {
            return (T) getLocalDateTime(columnIndex);

        } else if (type.equals(OffsetDateTime.class)) {
            checkRowPos();
            checkColumnBounds(columnIndex);
            return (T) this.thisRow.getValue(columnIndex - 1, this.defaultOffsetDateTimeValueFactory);

        } else if (type.equals(Boolean.class) || type.equals(Boolean.TYPE)) {
            checkRowPos();
            checkColumnBounds(columnIndex);
            return (T) this.thisRow.getValue(columnIndex - 1, this.booleanValueFactory);

        } else if (type.equals(Long.class) || type.equals(Long.TYPE)) {
            checkRowPos();
            checkColumnBounds(columnIndex);
            return (T) this.thisRow.getValue(columnIndex - 1, this.longValueFactory);

        } else if (type.equals(Double.class) || type.equals(Double.TYPE)) {
            checkRowPos();
            checkColumnBounds(columnIndex);
            return (T) this.thisRow.getValue(columnIndex - 1, this.doubleValueFactory);

        } else if (type.equals(BigDecimal.class)) {
            return (T) getBigDecimal(columnIndex);

        } else if (type.equals(BigInteger.class)) {
            return (T) getBigInteger(columnIndex);

        } else if (type.equals(Byte.class) || type.equals(Byte.TYPE)) {
            checkRowPos();
            checkColumnBounds(columnIndex);
            return (T) this.thisRow.getValue(columnIndex - 1, this.byteValueFactory);

        } else if (type.equals(Short.class) || type.equals(Short.TYPE)) {
            checkRowPos();
            checkColumnBounds(columnIndex);
            return (T) this.thisRow.getValue(columnIndex - 1, this.shortValueFactory);

        } else if (type.equals(Float.class) || type.equals(Float.TYPE)) {
            checkRowPos();
            checkColumnBounds(columnIndex);
            return (T) this.thisRow.getValue(columnIndex - 1, this.floatValueFactory);

        } else if (type.equals(byte[].class)) {
            return (T) getBytes(columnIndex);

        } else if (type.equals(Date.class)) {
            return (T) getDate(columnIndex);

        } else if (type.equals(Time.class)) {
            return (T) getTime(columnIndex);

        } else if (type.equals(Timestamp.class)) {
            return (T) getTimestamp(columnIndex);

        } else if (type.equals(java.util.Date.class)) {
            Timestamp ts = getTimestamp(columnIndex);
            return ts == null ? null : (T) java.util.Date.from(ts.toInstant());

        } else if (type.equals(Array.class)) {
            return (T) getArray(columnIndex);

        } else if (type.equals(Ref.class)) {
            return (T) getRef(columnIndex);

        } else if (type.equals(URL.class)) {
            return (T) getURL(columnIndex);

        } else if (type.equals(Struct.class)) {
            throw new SQLFeatureNotSupportedException();

        } else if (type.equals(RowId.class)) {
            return (T) getRowId(columnIndex);

        } else if (type.equals(NClob.class)) {
            return (T) getNClob(columnIndex);

        } else if (type.equals(SQLXML.class)) {
            return (T) getSQLXML(columnIndex);

        } else if (type.equals(LocalTime.class)) {
            return (T) getLocalTime(columnIndex);

        } else if (type.equals(OffsetTime.class)) {
            checkRowPos();
            checkColumnBounds(columnIndex);
            return (T) this.thisRow.getValue(columnIndex - 1, this.defaultOffsetTimeValueFactory);

        } else if (type.equals(ZonedDateTime.class)) {
            checkRowPos();
            checkColumnBounds(columnIndex);
            return (T) this.thisRow.getValue(columnIndex - 1, this.defaultZonedDateTimeValueFactory);

        }  else if (type.equals(Duration.class)) {
            checkRowPos();
            checkColumnBounds(columnIndex);
            return (T) this.thisRow.getValue(columnIndex - 1, new DurationValueFactory(this.session.getPropertySet()));
        }
        throw SQLError.createSQLException("Conversion not supported for type " + type.getName(), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT);
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return getObject(findColumn(columnLabel), type);
    }

    @Override
    public Object getObject(int i, java.util.Map<String, Class<?>> map) throws SQLException {
        return getObject(i);
    }

    @Override
    public Object getObject(String columnName) throws SQLException {
        return getObject(findColumn(columnName));
    }

    @Override
    public Object getObject(String colName, java.util.Map<String, Class<?>> map) throws SQLException {
        return getObject(findColumn(colName), map);
    }

    @Override
    public java.sql.Ref getRef(int i) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public java.sql.Ref getRef(String colName) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public int getRow() throws SQLException {
        checkClosed();

        if (!hasRows()) {
            throw SQLError.createSQLException(Messages.getString("ResultSet.ResultSet_is_from_UPDATE._No_Data_115"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR);
        }

        int currentRowNumber = this.rowData.getPosition();
        int row = 0;

        // Non-dynamic result sets can be interrogated for this information
        if (!this.rowData.isDynamic()) {
            if (currentRowNumber < 0 || this.rowData.isAfterLast() || this.rowData.isEmpty()) {
                row = 0;
            } else {
                row = currentRowNumber + 1;
            }
        } else {
            // dynamic (streaming) can not
            row = currentRowNumber + 1;
        }

        return row;
    }

    @Override
    public java.sql.Statement getStatement() throws SQLException {
        return this.owningStatement;
    }

    @Override
    public int getFetchSize() {
        return 0;
    }

    @Deprecated
    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        checkRowPos();

        return getBinaryStream(columnIndex);
    }

    @Deprecated
    @Override
    public InputStream getUnicodeStream(String columnName) throws SQLException {
        return getUnicodeStream(findColumn(columnName));
    }

    @Override
    public URL getURL(int colIndex) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public URL getURL(String colName) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public java.sql.SQLWarning getWarnings() throws SQLException {
        return this.warningChain;
    }

    @Override
    public void insertRow() throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public boolean isAfterLast() throws SQLException {
            if (!hasRows()) {
                throw SQLError.createSQLException(Messages.getString("ResultSet.ResultSet_is_from_UPDATE._No_Data_115"),
                        MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR);
            }
            return this.rowData.isAfterLast();

    }

    @Override
    public boolean isBeforeFirst() throws SQLException {

            if (!hasRows()) {
                throw SQLError.createSQLException(Messages.getString("ResultSet.ResultSet_is_from_UPDATE._No_Data_115"),
                        MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR);
            }

            return this.rowData.isBeforeFirst();

    }

    @Override
    public boolean isFirst() throws SQLException {
            if (!hasRows()) {
                throw SQLError.createSQLException(Messages.getString("ResultSet.ResultSet_is_from_UPDATE._No_Data_115"),
                        MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR);
            }

            return this.rowData.isFirst();
    }

    @Override
    public boolean isLast() throws SQLException {
            if (!hasRows()) {
                throw SQLError.createSQLException(Messages.getString("ResultSet.ResultSet_is_from_UPDATE._No_Data_115"),
                        MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR);
            }

            return this.rowData.isLast();
    }

    @Override
    public int getType() throws SQLException {
        return TYPE_FORWARD_ONLY;
    }

    @Override
    public boolean last() throws SQLException {
        if (!hasRows()) {
            throw SQLError.createSQLException(Messages.getString("ResultSet.ResultSet_is_from_UPDATE._No_Data_115"),
                    MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR);
        }

        if (isStrictlyForwardOnly()) {
            throw SQLExceptionsMapping.translateException(ExceptionFactory.createException(Messages.getString("ResultSet.ForwardOnly")));
        }

        boolean b = true;

        if (this.rowData.size() == 0) {
            b = false;
        } else {
            this.rowData.beforeLast();
            this.thisRow = this.rowData.next();
        }

        setRowPositionValidity();

        return b;
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public boolean next() throws SQLException {
        if (!hasRows()) {
            throw SQLError.createSQLException(Messages.getString("ResultSet.ResultSet_is_from_UPDATE._No_Data_115"),
                    MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR);
        }
        boolean b;
        if (this.rowData.size() == 0) {
            b = false;
        } else {
            this.thisRow = this.rowData.next();

            if (this.thisRow == null) {
                b = false;
            } else {
                clearWarnings();

                b = true;

            }
        }
        setRowPositionValidity();
        return b;
    }

    /**
     * The <i>prev</i> method is not part of JDBC, but because of the architecture of this driver it is possible to move both forward and backward within the
     * result set.
     *
     * <p>
     * If an input stream from the previous row is open, it is implicitly closed. The ResultSet's warning chain is cleared when a new row is read
     * </p>
     *
     * @return true if the new current is valid; false if there are no more rows
     *
     * @exception java.sql.SQLException
     *                if a database access error occurs
     */
    public boolean prev() throws java.sql.SQLException {
        int rowIndex = this.rowData.getPosition();
        boolean b = true;
        if (rowIndex - 1 >= 0) {
            rowIndex--;
            this.rowData.setCurrentRow(rowIndex);
            this.thisRow = this.rowData.get(rowIndex);

            b = true;
        } else if (rowIndex - 1 == -1) {
            rowIndex--;
            this.rowData.setCurrentRow(rowIndex);
            this.thisRow = null;

            b = false;
        } else {
            b = false;
        }
        setRowPositionValidity();
        return b;
    }

    @Override
    public boolean previous() throws SQLException {
        if (!hasRows()) {
            throw SQLError.createSQLException(Messages.getString("ResultSet.ResultSet_is_from_UPDATE._No_Data_115"),
                    MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR);
        }

        if (isStrictlyForwardOnly()) {
            throw SQLExceptionsMapping.translateException(ExceptionFactory.createException(Messages.getString("ResultSet.ForwardOnly")));
        }

        return prev();
    }

    @Override
    public void realClose(boolean calledExplicitly) throws SQLException {
        JdbcConnection locallyScopedConn = this.connection;

        if (locallyScopedConn == null) {
            return; // already closed
        }


            // additional check in case ResultSet was closed while current thread was waiting for lock
            if (this.isClosed) {
                return;
            }

        if (this.owningStatement != null && calledExplicitly) {
            this.owningStatement.removeOpenResultSet(this);
        }

        this.rowData = null;
        this.columnDefinition = null;
        this.warningChain = null;
        this.owningStatement = null;
        this.serverInfo = null;
        this.thisRow = null;
        this.connection = null;
        this.session = null;

        this.isClosed = true;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.isClosed;
    }

    @Override
    public void refreshRow() throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        if (!hasRows()) {
            throw SQLError.createSQLException(Messages.getString("ResultSet.ResultSet_is_from_UPDATE._No_Data_115"),
                    MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR);
        }

        if (isStrictlyForwardOnly()) {
            throw SQLExceptionsMapping.translateException(ExceptionFactory.createException(Messages.getString("ResultSet.ForwardOnly")));
        }

        if (this.rowData.size() == 0) {
            setRowPositionValidity();

            return false;
        }

        this.rowData.moveRowRelative(rows);
        this.thisRow = this.rowData.get(this.rowData.getPosition());

        setRowPositionValidity();

        return !this.rowData.isAfterLast() && !this.rowData.isBeforeFirst();
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public boolean rowInserted() throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    /**
     * Checks whether this ResultSet is scrollable even if its type is ResultSet.TYPE_FORWARD_ONLY. Required for backwards compatibility.
     *
     * @return
     *         <code>true</code> if this result set type is ResultSet.TYPE_FORWARD_ONLY and the connection property 'scrollTolerantForwardOnly' has not been set
     *         to <code>true</code>.
     */
    protected boolean isStrictlyForwardOnly() {
        return true;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        if (rows < 0  && rows != Integer.MIN_VALUE) { /* || rows > getMaxRows() */
            throw SQLError.createSQLException(Messages.getString("ResultSet.Value_must_be_between_0_and_getMaxRows()_66"),
                    MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT);
        }
    }

    @Override
    public void setFirstCharOfQuery(char c) {
            this.firstCharOfQuery = c;
    }

    @Override
    public String toString() {
        return hasRows() ? super.toString() : "Result set representing update count of " + this.updateCount;
    }

    @Override
    public void updateArray(int columnIndex, Array arg1) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public void updateArray(String columnLabel, Array arg1) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateAsciiStream(String columnName, InputStream x, int length) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateBigDecimal(String columnName, BigDecimal x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateBinaryStream(String columnName, InputStream x, int length) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateBlob(int columnIndex, java.sql.Blob arg1) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateBlob(String columnLabel, java.sql.Blob arg1) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateBoolean(String columnName, boolean x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateByte(String columnName, byte x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateBytes(String columnName, byte[] x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateCharacterStream(String columnName, Reader reader, int length) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateClob(int columnIndex, java.sql.Clob arg1) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(String columnName, java.sql.Clob clob) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateDate(int columnIndex, java.sql.Date x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateDate(String columnName, java.sql.Date x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateDouble(String columnName, double x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateFloat(String columnName, float x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateInt(String columnName, int x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateLong(String columnName, long x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateNClob(String columnName, NClob nClob) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateNull(String columnName) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateObject(String columnName, Object x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scale) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateObject(String columnName, Object x, int scale) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateObject(int columnIndex, Object x, SQLType targetSqlType) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateObject(int columnIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateObject(String columnLabel, Object x, SQLType targetSqlType) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateObject(String columnLabel, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateRef(int columnIndex, Ref arg1) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public void updateRef(String columnLabel, Ref arg1) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public void updateRow() throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public void updateRowId(String columnName, RowId x) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateShort(String columnName, short x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateString(String columnName, String x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateTime(int columnIndex, java.sql.Time x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateTime(String columnName, java.sql.Time x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateTimestamp(int columnIndex, java.sql.Timestamp x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public void updateTimestamp(String columnName, java.sql.Timestamp x) throws SQLException {
        throw new NotUpdatable(Messages.getString("NotUpdatable.0"));
    }

    @Override
    public boolean wasNull() throws SQLException {
        return this.thisRow.wasNull();
    }

    @Override
    public int getHoldability() throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        return getSQLXML(findColumn(columnLabel));
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        checkClosed();

        // This works for classes that aren't actually wrapping anything
        return iface.isInstance(this);
    }

    @Override
    public <T> T unwrap(java.lang.Class<T> iface) throws java.sql.SQLException {
        try {
            // This works for classes that aren't actually wrapping anything
            return iface.cast(this);
        } catch (ClassCastException cce) {
            throw SQLError.createSQLException(Messages.getString("Common.UnableToUnwrap", new Object[] { iface.toString() }),
                    MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT);
        }
    }

    /**
     * Accumulate internal warnings as the SQLWarning chain.
     */
    @Override
    public void warningEncountered(String warning) {
        SQLWarning w = new SQLWarning(warning);
        if (this.warningChain == null) {
            this.warningChain = w;
        } else {
            this.warningChain.setNextWarning(w);
        }
    }

    public ColumnDefinition getMetadata() {
        return this.columnDefinition;
    }

    public com.mysql.cj.jdbc.StatementImpl getOwningStatement() {
        return this.owningStatement;
    }

    @Override
    public JdbcConnection getConnection() {
        return this.connection;
    }

    @Override
    public Session getSession() {
        return this.connection != null ? this.connection.getSession() : null;
    }


}
