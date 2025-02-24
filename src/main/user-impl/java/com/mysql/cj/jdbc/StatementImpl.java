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

package com.mysql.cj.jdbc;

import com.mysql.cj.AbstractQuery;
import com.mysql.cj.CancelQueryTask;
import com.mysql.cj.Messages;
import com.mysql.cj.MysqlType;
import com.mysql.cj.NativeSession;
import com.mysql.cj.Query;
import com.mysql.cj.Session;
import com.mysql.cj.SimpleQuery;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.RuntimeProperty;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.exceptions.StatementIsClosedException;
import com.mysql.cj.jdbc.exceptions.MySQLStatementCancelledException;
import com.mysql.cj.jdbc.exceptions.MySQLTimeoutException;
import com.mysql.cj.jdbc.exceptions.SQLError;
import com.mysql.cj.jdbc.exceptions.SQLExceptionsMapping;
import com.mysql.cj.jdbc.result.ResultSetFactory;
import com.mysql.cj.jdbc.result.ResultSetImpl;
import com.mysql.cj.jdbc.result.ResultSetInternalMethods;
import com.mysql.cj.protocol.Message;
import com.mysql.cj.protocol.ProtocolEntityFactory;
import com.mysql.cj.protocol.Resultset;
import com.mysql.cj.protocol.a.NativeMessageBuilder;
import com.mysql.cj.protocol.a.result.ByteArrayRow;
import com.mysql.cj.protocol.a.result.ResultsetRowsStatic;
import com.mysql.cj.result.DefaultColumnDefinition;
import com.mysql.cj.result.Field;
import com.mysql.cj.result.Row;
import com.mysql.cj.util.StringUtils;
import com.mysql.cj.util.Util;

import java.io.IOException;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A Statement object is used for executing a static SQL statement and obtaining
 * the results produced by it.
 *
 * Only one ResultSet per Statement can be open at any point in time. Therefore, if the reading of one ResultSet is interleaved with the reading of another,
 * each must have been generated by different Statements. All statement execute methods implicitly close a statement's current ResultSet if an open one exists.
 */
public class StatementImpl implements JdbcStatement {
    public final static byte USES_VARIABLES_FALSE = 0;

    public final static byte USES_VARIABLES_TRUE = 1;

    /** The character encoding to use (if available) */
    protected String charEncoding = null;

    /** The connection that created us */
    protected volatile JdbcConnection connection = null;

    /** Should we process escape codes? */
    protected boolean doEscapeProcessing = true;

    /** Has this statement been closed? */
    protected boolean isClosed = false;

    /** The auto_increment value for the last insert */
    protected long lastInsertId = -1;

    /** The max field size for this statement */
    protected int maxFieldSize;

    /** The current results */
    protected ResultSetInternalMethods results = null;

    /** The update count for this statement */
    protected long updateCount = -1;

    /** The warnings chain. */
    protected SQLWarning warningChain = null;

    protected ArrayList<Row> batchedGeneratedKeys = null;

    protected boolean retrieveGeneratedKeys = false;

    protected boolean continueBatchOnError = false;

    /** Whether or not the last query was of the form ON DUPLICATE KEY UPDATE */
    protected boolean lastQueryIsOnDupKeyUpdate = false;

    protected RuntimeProperty<Boolean> rewriteBatchedStatements;
    protected RuntimeProperty<Integer> maxAllowedPacket;
    protected boolean dontCheckOnDuplicateKeyUpdateInSQL;

    protected ResultSetFactory resultSetFactory;

    protected Query query;
    protected NativeSession session = null;

    /**
     * Constructor for a Statement.
     *
     * @param c
     *            the Connection instance that creates us
     * @throws SQLException
     *             if an error occurs.
     */
    public StatementImpl(JdbcConnection c) throws SQLException {
        if (c == null || c.isClosed()) {
            throw SQLError.createSQLException(Messages.getString("Statement.0"), MysqlErrorNumbers.SQL_STATE_CONNECTION_NOT_OPEN);
        }

        this.connection = c;
        this.session = (NativeSession) c.getSession();

        initQuery();

        JdbcPropertySet pset = c.getPropertySet();

        this.continueBatchOnError = pset.getBooleanProperty(PropertyKey.continueBatchOnError).getValue();

        this.rewriteBatchedStatements = pset.getBooleanProperty(PropertyKey.rewriteBatchedStatements);
        this.charEncoding = pset.getStringProperty(PropertyKey.characterEncoding).getValue();

        this.maxAllowedPacket = pset.getIntegerProperty(PropertyKey.maxAllowedPacket);
        this.dontCheckOnDuplicateKeyUpdateInSQL = pset.getBooleanProperty(PropertyKey.dontCheckOnDuplicateKeyUpdateInSQL).getValue();
        this.doEscapeProcessing = pset.getBooleanProperty(PropertyKey.enableEscapeProcessing).getValue();

        this.maxFieldSize = this.maxAllowedPacket.getValue();

        this.resultSetFactory = new ResultSetFactory(this.connection, this);
    }

    protected void initQuery() {
        this.query = new SimpleQuery(this.session);
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public void cancel() throws SQLException {
        if (!this.query.getStatementExecuting().get()) {
            return;
        }

        if (!this.isClosed && this.connection != null) {
            NativeSession newSession = null;

            try {
                HostInfo hostInfo = this.session.getHostInfo();
                String database = hostInfo.getDatabase();
                String user = hostInfo.getUser();
                String password = hostInfo.getPassword();
                newSession = new NativeSession(this.session.getHostInfo(), this.session.getPropertySet());
                newSession.connect(hostInfo, user, password, database, 30000);
                newSession.getProtocol().sendCommand(new NativeMessageBuilder(newSession.getServerSession().supportsQueryAttributes())
                        .buildComQuery(newSession.getSharedSendPacket(), "KILL QUERY " + this.session.getThreadId()), false, 0);
                setCancelStatus(CancelStatus.CANCELED_BY_USER);
            } catch (CJException | IOException e) {
                throw SQLExceptionsMapping.translateException(e);
            } finally {
                if (newSession != null) {
                    newSession.forceClose();
                }
            }

        }
    }

    // --------------------------JDBC 2.0-----------------------------

    /**
     * Checks if closed() has been called, and throws an exception if so
     *
     * @return connection
     * @throws StatementIsClosedException
     *             if this statement has been closed
     */
    protected JdbcConnection checkClosed() throws SQLException {
        JdbcConnection c = this.connection;

        if (c == null) {
            throw SQLExceptionsMapping.translateException(new StatementIsClosedException(Messages.getString("Statement.AlreadyClosed")));
        }

        return c;
    }

    @Override
    public void clearBatch() {
        ((AbstractQuery) this.query).clearBatchedArgs();
    }

    @Override
    public void clearWarnings() {
        setClearWarningsCalled(true);
        this.warningChain = null;
        // TODO souldn't we also clear warnings from _server_ ?
    }

    /**
     * In many cases, it is desirable to immediately release a Statement's
     * database and JDBC resources instead of waiting for this to happen when it
     * is automatically closed. The close method provides this immediate
     * release.
     *
     * <p>
     * <B>Note:</B> A Statement is automatically closed when it is garbage collected. When a Statement is closed, its current ResultSet, if one exists, is also
     * closed.
     * </p>
     *
     * @exception SQLException
     *                if a database access error occurs
     */
    @Override
    public void close() throws SQLException {
        realClose(true, true);
    }

    @Override
    public void removeOpenResultSet(ResultSetInternalMethods rs) {

        boolean hasMoreResults = rs.getNextResultset() != null;

        // clear the current results or GGK results
        if (this.results == rs && !hasMoreResults) {
            this.results = null;
        }
    }

    @Override
    public CancelQueryTask startQueryTimer(Query stmtToCancel, long timeout) {
        return this.query.startQueryTimer(stmtToCancel, timeout);
    }

    @Override
    public void stopQueryTimer(CancelQueryTask timeoutTask, boolean rethrowCancelReason, boolean checkCancelTimeout) throws CJException {
        this.query.stopQueryTimer(timeoutTask, rethrowCancelReason, checkCancelTimeout);
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public boolean execute(String sql, int returnGeneratedKeys) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public boolean execute(String sql, int[] generatedKeyIndices) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public boolean execute(String sql, String[] generatedKeyNames) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public void statementBegins() {
        this.query.statementBegins();
    }

    @Override
    public void resetCancelledState() {
        this.query.resetCancelledState();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        return Util.truncateAndConvertToInt(executeBatchInternal());
    }

    protected long[] executeBatchInternal() throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    protected final boolean hasDeadlockOrTimeoutRolledBackTx(SQLException ex) {
        int vendorCode = ex.getErrorCode();

        switch (vendorCode) {
            case MysqlErrorNumbers.ER_LOCK_DEADLOCK:
            case MysqlErrorNumbers.ER_LOCK_TABLE_FULL:
                return true;
            case MysqlErrorNumbers.ER_LOCK_WAIT_TIMEOUT:
                return false;
            default:
                return false;
        }
    }

    protected int processMultiCountsAndKeys(StatementImpl batchedStatement, int updateCountCounter, long[] updateCounts) throws SQLException {

            updateCounts[updateCountCounter++] = batchedStatement.getLargeUpdateCount();

            boolean doGenKeys = this.batchedGeneratedKeys != null;

            byte[][] row = null;

            if (doGenKeys) {
                long generatedKey = batchedStatement.getLastInsertID();

                row = new byte[1][];
                row[0] = StringUtils.getBytes(Long.toString(generatedKey));
                this.batchedGeneratedKeys.add(new ByteArrayRow(row));
            }

            while (batchedStatement.getMoreResults() || batchedStatement.getLargeUpdateCount() != -1) {
                updateCounts[updateCountCounter++] = batchedStatement.getLargeUpdateCount();

                if (doGenKeys) {
                    long generatedKey = batchedStatement.getLastInsertID();

                    row = new byte[1][];
                    row[0] = StringUtils.getBytes(Long.toString(generatedKey));
                    this.batchedGeneratedKeys.add(new ByteArrayRow(row));
                }
            }

            return updateCountCounter;

    }

    protected SQLException handleExceptionForBatch(int endOfBatchIndex, int numValuesPerBatch, long[] updateCounts, SQLException ex)
            throws SQLException {
        for (int j = endOfBatchIndex; j > endOfBatchIndex - numValuesPerBatch; j--) {
            updateCounts[j] = EXECUTE_FAILED;
        }

        if (this.continueBatchOnError && !(ex instanceof MySQLTimeoutException) && !(ex instanceof MySQLStatementCancelledException)
                && !hasDeadlockOrTimeoutRolledBackTx(ex)) {
            return ex;
        } // else: throw the exception immediately

        long[] newUpdateCounts = new long[endOfBatchIndex];
        System.arraycopy(updateCounts, 0, newUpdateCounts, 0, endOfBatchIndex);

        throw SQLError.createBatchUpdateException(ex, newUpdateCounts);
    }

    @Override
    public java.sql.ResultSet executeQuery(String sql) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public java.sql.Connection getConnection() {
        return this.connection;
    }

    @Override
    public int getFetchDirection() {
        return java.sql.ResultSet.FETCH_FORWARD;
    }

    @Override
    public int getFetchSize() throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public java.sql.ResultSet getGeneratedKeys() throws SQLException {

            if (!this.retrieveGeneratedKeys) {
                throw SQLError.createSQLException(Messages.getString("Statement.GeneratedKeysNotRequested"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT);
            }

            if (this.batchedGeneratedKeys == null) {
                if (this.lastQueryIsOnDupKeyUpdate) {
                    return getGeneratedKeysInternal(1);
                }
                return getGeneratedKeysInternal();
            }

            String encoding = this.session.getServerSession().getCharsetSettings().getMetadataEncoding();
            int collationIndex = this.session.getServerSession().getCharsetSettings().getMetadataCollationIndex();
            Field[] fields = new Field[1];
            fields[0] = new Field("", "GENERATED_KEY", collationIndex, encoding, MysqlType.BIGINT_UNSIGNED, 20);

        return this.resultSetFactory.createFromResultsetRows(
                    new ResultsetRowsStatic(this.batchedGeneratedKeys, new DefaultColumnDefinition(fields)));
    }

    /*
     * Needed because there's no concept of super.super to get to this
     * implementation from ServerPreparedStatement when dealing with batched
     * updates.
     */
    protected ResultSetInternalMethods getGeneratedKeysInternal() throws SQLException {
        long numKeys = getLargeUpdateCount();
        return getGeneratedKeysInternal(numKeys);
    }

    protected ResultSetInternalMethods getGeneratedKeysInternal(long numKeys) throws SQLException {

            String encoding = this.session.getServerSession().getCharsetSettings().getMetadataEncoding();
            int collationIndex = this.session.getServerSession().getCharsetSettings().getMetadataCollationIndex();
            Field[] fields = new Field[1];
            fields[0] = new Field("", "GENERATED_KEY", collationIndex, encoding, MysqlType.BIGINT_UNSIGNED, 20);

            ArrayList<Row> rowSet = new ArrayList<>();

            long beginAt = getLastInsertID();

            if (this.results != null) {
                String serverInfo = this.results.getServerInfo();

                //
                // Only parse server info messages for 'REPLACE' queries
                //
                if (numKeys > 0 && this.results.getFirstCharOfQuery() == 'R' && serverInfo != null && serverInfo.length() > 0) {
                    numKeys = getRecordCountFromInfo(serverInfo);
                }

                if (beginAt != 0 /* BIGINT UNSIGNED can wrap the protocol representation */ && numKeys > 0) {
                    for (int i = 0; i < numKeys; i++) {
                        byte[][] row = new byte[1][];
                        if (beginAt > 0) {
                            row[0] = StringUtils.getBytes(Long.toString(beginAt));
                        } else {
                            byte[] asBytes = new byte[8];
                            asBytes[7] = (byte) (beginAt & 0xff);
                            asBytes[6] = (byte) (beginAt >>> 8);
                            asBytes[5] = (byte) (beginAt >>> 16);
                            asBytes[4] = (byte) (beginAt >>> 24);
                            asBytes[3] = (byte) (beginAt >>> 32);
                            asBytes[2] = (byte) (beginAt >>> 40);
                            asBytes[1] = (byte) (beginAt >>> 48);
                            asBytes[0] = (byte) (beginAt >>> 56);

                            BigInteger val = new BigInteger(1, asBytes);

                            row[0] = val.toString().getBytes();
                        }
                        rowSet.add(new ByteArrayRow(row));
                        beginAt += this.connection.getAutoIncrementIncrement();
                    }
                }
            }

        ResultSetImpl gkRs = this.resultSetFactory.createFromResultsetRows(
                    new ResultsetRowsStatic(rowSet, new DefaultColumnDefinition(fields)));

            return gkRs;

    }

    /**
     * getLastInsertID returns the value of the auto_incremented key after an
     * executeQuery() or excute() call.
     *
     * <p>
     * This gets around the un-threadsafe behavior of "select LAST_INSERT_ID()" which is tied to the Connection that created this Statement, and therefore could
     * have had many INSERTS performed before one gets a chance to call "select LAST_INSERT_ID()".
     * </p>
     *
     * @return the last update ID.
     */
    public long getLastInsertID() {
        return this.lastInsertId;
    }

    @Override
    public int getMaxFieldSize() {
        return this.maxFieldSize;
    }

    @Override
    public int getMaxRows() {
        return 0;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return getMoreResults(CLOSE_CURRENT_RESULT);
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        if (this.results == null) {
            return false;
        }

        this.results.realClose(false);

        this.results = (ResultSetInternalMethods) this.results.getNextResultset();

        if (this.results == null) {
            this.updateCount = -1;
            this.lastInsertId = -1;
        } else if (this.results.hasRows()) {
            this.updateCount = -1;
            this.lastInsertId = -1;
        } else {
            this.updateCount = this.results.getUpdateCount();
            this.lastInsertId = this.results.getUpdateID();
        }

        return this.results != null && this.results.hasRows();
    }

    @Override
    public int getQueryTimeout() {
        return Math.toIntExact(getTimeoutInMillis() / 1000);
    }

    /**
     * Parses actual record count from 'info' message
     *
     * @param serverInfo
     *            server info message
     * @return records count
     */
    private long getRecordCountFromInfo(String serverInfo) {
        StringBuilder recordsBuf = new StringBuilder();
        long recordsCount = 0;
        long duplicatesCount = 0;

        char c = (char) 0;

        int length = serverInfo.length();
        int i = 0;

        for (; i < length; i++) {
            c = serverInfo.charAt(i);

            if (Character.isDigit(c)) {
                break;
            }
        }

        recordsBuf.append(c);
        i++;

        for (; i < length; i++) {
            c = serverInfo.charAt(i);

            if (!Character.isDigit(c)) {
                break;
            }

            recordsBuf.append(c);
        }

        recordsCount = Long.parseLong(recordsBuf.toString());

        StringBuilder duplicatesBuf = new StringBuilder();

        for (; i < length; i++) {
            c = serverInfo.charAt(i);

            if (Character.isDigit(c)) {
                break;
            }
        }

        duplicatesBuf.append(c);
        i++;

        for (; i < length; i++) {
            c = serverInfo.charAt(i);

            if (!Character.isDigit(c)) {
                break;
            }

            duplicatesBuf.append(c);
        }

        duplicatesCount = Long.parseLong(duplicatesBuf.toString());

        return recordsCount - duplicatesCount;
    }

    @Override
    public java.sql.ResultSet getResultSet() {
        return this.results != null && this.results.hasRows() ? this.results : null;
    }

    @Override
    public int getResultSetConcurrency() {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetHoldability() {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getResultSetType() {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return Util.truncateAndConvertToInt(getLargeUpdateCount());
    }

    @Override
    public java.sql.SQLWarning getWarnings() throws SQLException {


            if (isClearWarningsCalled()) {
                return null;
            }

        SQLWarning pendingWarningsFromServer = null;
        try {
            pendingWarningsFromServer = this.session.getProtocol().convertShowWarningsToSQLWarnings(false);
        } catch (CJException e) {
            throw SQLExceptionsMapping.translateException(e);
        }

        if (this.warningChain != null) {
                this.warningChain.setNextWarning(pendingWarningsFromServer);
            } else {
                this.warningChain = pendingWarningsFromServer;
            }

            return this.warningChain;

    }

    /**
     * Closes this statement, and frees resources.
     *
     * @param calledExplicitly
     *            was this called from close()?
     * @param closeOpenResults
     *            should open result sets be closed?
     *
     * @throws SQLException
     *             if an error occurs
     */
    protected void realClose(boolean calledExplicitly, boolean closeOpenResults) throws SQLException {
        JdbcConnection locallyScopedConn = this.connection;

        if (locallyScopedConn == null || this.isClosed) {
            return; // already closed
        }

        if (closeOpenResults) {
            if (this.results != null) {
                try {
                    this.results.close();
                } catch (Exception ex) {
                }
            }
        }

        this.isClosed = true;

        closeQuery();

        this.results = null;
        this.connection = null;
        this.session = null;
        this.warningChain = null;
        this.batchedGeneratedKeys = null;
        this.resultSetFactory = null;
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        // No-op
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {

            this.doEscapeProcessing = enable;

    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        switch (direction) {
            case java.sql.ResultSet.FETCH_FORWARD:
            case java.sql.ResultSet.FETCH_REVERSE:
            case java.sql.ResultSet.FETCH_UNKNOWN:
                break;

            default:
                throw SQLError.createSQLException(Messages.getString("Statement.5"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT);
        }
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {

            if (max < 0) {
                throw SQLError.createSQLException(Messages.getString("Statement.11"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT);
            }

            int maxBuf = this.maxAllowedPacket.getValue();

            if (max > maxBuf) {
                throw SQLError.createSQLException(Messages.getString("Statement.13", new Object[] { Long.valueOf(maxBuf) }),
                        MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT);
            }

            this.maxFieldSize = max;

    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        setLargeMaxRows(max);
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {

            if (seconds < 0) {
                throw SQLError.createSQLException(Messages.getString("Statement.21"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT);
            }

            setTimeoutInMillis(seconds * 1000);

    }

    protected void getBatchedGeneratedKeys(java.sql.Statement batchedStatement) throws SQLException {

            if (this.retrieveGeneratedKeys) {
                java.sql.ResultSet rs = null;

                try {
                    rs = batchedStatement.getGeneratedKeys();

                    while (rs.next()) {
                        this.batchedGeneratedKeys.add(new ByteArrayRow(new byte[][] { rs.getBytes(1) }));
                    }
                } finally {
                    if (rs != null) {
                        rs.close();
                    }
                }
            }

    }

    protected void getBatchedGeneratedKeys(int maxKeys) throws SQLException {

            if (this.retrieveGeneratedKeys) {
                java.sql.ResultSet rs = null;

                try {
                    rs = maxKeys == 0 ? getGeneratedKeysInternal() : getGeneratedKeysInternal(maxKeys);
                    while (rs.next()) {
                        this.batchedGeneratedKeys.add(new ByteArrayRow(new byte[][] { rs.getBytes(1) }));
                    }
                } finally {
                    if (rs != null) {
                        rs.close();
                    }
                }
            }

    }

    @Override
    public boolean isClosed() throws SQLException {
        JdbcConnection locallyScopedConn = this.connection;
        if (locallyScopedConn == null) {
            return true;
        }

            return this.isClosed;

    }

    private boolean isPoolable = false;

    @Override
    public boolean isPoolable() throws SQLException {
        checkClosed();
        return this.isPoolable;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        checkClosed();
        this.isPoolable = poolable;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        checkClosed();

        // This works for classes that aren't actually wrapping anything
        return iface.isInstance(this);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            // This works for classes that aren't actually wrapping anything
            return iface.cast(this);
        } catch (ClassCastException cce) {
            throw SQLError.createSQLException(Messages.getString("Common.UnableToUnwrap", new Object[] { iface.toString() }),
                    MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT);
        }
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public long getLargeMaxRows() {
        // Max rows is limited by MySQLDefs.MAX_ROWS anyway...
        return getMaxRows();
    }

    @Override
    public long getLargeUpdateCount() {

            if (this.results == null || this.results.hasRows()) {
                return -1;
            }

            return this.results.getUpdateCount();

    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends Resultset, M extends Message> ProtocolEntityFactory<T, M> getResultSetFactory() {
        return (ProtocolEntityFactory<T, M>) this.resultSetFactory;
    }

    @Override
    public void setCancelStatus(CancelStatus cs) {
        this.query.setCancelStatus(cs);
    }

    @Override
    public void checkCancelTimeout() throws CJException {
        this.query.checkCancelTimeout();
    }

    @Override
    public Session getSession() {
        return this.session;
    }

    @Override
    public ReentrantLock getCancelTimeoutMutex() {
        return this.query.getCancelTimeoutMutex();
    }

    @Override
    public void closeQuery() {
        if (this.query != null) {
            this.query.closeQuery();
        }
    }

    @Override
    public long getTimeoutInMillis() {
        return this.query.getTimeoutInMillis();
    }

    @Override
    public void setTimeoutInMillis(long timeoutInMillis) {
        this.query.setTimeoutInMillis(timeoutInMillis);
    }

    @Override
    public AtomicBoolean getStatementExecuting() {
        return this.query.getStatementExecuting();
    }

    @Override
    public boolean isClearWarningsCalled() {
        return this.query.isClearWarningsCalled();
    }

    @Override
    public void setClearWarningsCalled(boolean clearWarningsCalled) {
        this.query.setClearWarningsCalled(clearWarningsCalled);
    }
}
