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

import com.mysql.cj.CacheAdapter;
import com.mysql.cj.CacheAdapterFactory;
import com.mysql.cj.Messages;
import com.mysql.cj.NativeSession;
import com.mysql.cj.PerConnectionLRUFactory;
import com.mysql.cj.QueryInfo;
import com.mysql.cj.Session.SessionEventListener;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.RuntimeProperty;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.ConnectionIsClosedException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.exceptions.OperationCancelledException;
import com.mysql.cj.exceptions.PasswordExpiredException;
import com.mysql.cj.exceptions.UnableToConnectException;
import com.mysql.cj.jdbc.exceptions.SQLError;
import com.mysql.cj.jdbc.exceptions.SQLExceptionsMapping;
import com.mysql.cj.jdbc.result.ResultSetFactory;
import com.mysql.cj.protocol.a.NativeProtocol;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.DriverManager;
import java.sql.NClob;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * A Connection represents a session with a specific database. Within the context of a Connection, SQL statements are executed and results are returned.
 *
 * <P>
 * A Connection's database is able to provide information describing its tables, its supported SQL grammar, its stored procedures, the capabilities of this
 * connection, etc. This information is obtained with the getMetaData method.
 * </p>
 */
public class ConnectionImpl implements JdbcConnection, SessionEventListener {
    /**
     * Map mysql transaction isolation level name to
     * java.sql.Connection.TRANSACTION_XXX
     */
    private static final Map<String, Integer> mapTransIsolationNameToValue = Map.of("READ-UNCOMMITED", TRANSACTION_READ_UNCOMMITTED,
            "READ-UNCOMMITTED", TRANSACTION_READ_UNCOMMITTED,
            "READ-COMMITTED", TRANSACTION_READ_COMMITTED,
            "REPEATABLE-READ", TRANSACTION_REPEATABLE_READ,
            "SERIALIZABLE", TRANSACTION_SERIALIZABLE);

    /**
     * Creates a connection instance.
     *
     * @param hostInfo
     *            {@link HostInfo} instance
     * @return new {@link ConnectionImpl} instance
     * @throws SQLException
     *             if a database access error occurs
     */
    public static JdbcConnection getInstance(HostInfo hostInfo) throws SQLException {
        return new ConnectionImpl(hostInfo);
    }

    /** A cache of SQL to parsed prepared statement parameters. */
    private CacheAdapter<String, QueryInfo> queryInfoCache;

    /** The database we're currently using. */
    private String database = null;

    private NativeSession session = null;

    /** isolation level */
    private int isolationLevel = java.sql.Connection.TRANSACTION_READ_COMMITTED;

    /** The password we used */
    private String password = null;

    /** Properties for this connection specified by user */
    protected Properties props = null;

    /** The user we're connected as */
    private String user = null;

    private final HostInfo origHostInfo;

    protected JdbcPropertySet propertySet;

    private final RuntimeProperty<Boolean> cachePrepStmts;

    private final RuntimeProperty<Integer> prepStmtCacheSqlLimit;

    private final RuntimeProperty<Boolean> disconnectOnExpiredPasswords;

    protected ResultSetFactory nullStatementResultSetFactory;

    /**
     * Creates a connection to a MySQL Server.
     *
     * @param hostInfo
     *            the {@link HostInfo} instance that contains the host, user and connections attributes for this connection
     * @exception SQLException
     *                if a database access error occurs
     */
    public ConnectionImpl(HostInfo hostInfo) throws SQLException {
        // Stash away for later, used to clone this connection for Statement.cancel and Statement.setQueryTimeout().
        this.origHostInfo = hostInfo;

        this.database = hostInfo.getDatabase();
        this.user = hostInfo.getUser();
        this.password = hostInfo.getPassword();

        this.props = hostInfo.exposeAsProperties();
        this.propertySet = new JdbcPropertySetImpl();
        this.propertySet.initializeProperties(this.props);

        // We need Session ASAP to get access to central driver functionality
        this.nullStatementResultSetFactory = new ResultSetFactory(this, null);
        this.session = new NativeSession(hostInfo, this.propertySet);
        this.session.addListener(this); // listen for session status changes

        this.cachePrepStmts = this.propertySet.getBooleanProperty(PropertyKey.cachePrepStmts);
        this.prepStmtCacheSqlLimit = this.propertySet.getIntegerProperty(PropertyKey.prepStmtCacheSqlLimit);

        this.disconnectOnExpiredPasswords = this.propertySet.getBooleanProperty(PropertyKey.disconnectOnExpiredPasswords);

        if (this.cachePrepStmts.getValue()) {
            createPreparedStatementCaches();
        }

        try {
            createNewIO();
        } catch (Exception ex) {
            cleanup(ex);

            throw SQLError.createSQLException(Messages.getString("Connection.1",
                                    new Object[] { this.session.getHostInfo().getHost(), String.valueOf(this.session.getHostInfo().getPort()) }),
                            MysqlErrorNumbers.SQL_STATE_COMMUNICATION_LINK_FAILURE, ex);
        }
    }

    @Override
    public JdbcPropertySet getPropertySet() {
        return this.propertySet;
    }

    @Override
    public void checkClosed() throws SQLException {
        try {
            this.session.checkClosed();
        } catch (ConnectionIsClosedException|OperationCancelledException e) {
            throw SQLExceptionsMapping.translateException(e);
        }
    }

    /**
     * Set transaction isolation level to the value received from server if any.
     * Is called by connectionInit(...)
     */
    private void checkTransactionIsolationLevel() {
        String s = this.session.getServerSession().getServerVariable("transaction_isolation");
        if (s == null) {
            s = this.session.getServerSession().getServerVariable("tx_isolation");
        }

        if (s != null) {
            Integer intTI = mapTransIsolationNameToValue.get(s);

            if (intTI != null) {
                this.isolationLevel = intTI.intValue();
            }
        }
    }

    @Override
    public void abortInternal() throws SQLException {
        this.session.forceClose();
    }

    @Override
    public void cleanup(Throwable whyCleanedUp) {
        try {
            if (this.session != null) {
                if (isClosed()) {
                    this.session.forceClose();
                } else {
                    realClose(false, false, false, whyCleanedUp);
                }
            }
        } catch (SQLException sqlEx) {
            // ignore, we're going away.
        }
    }

    @Override
    public void clearWarnings() throws SQLException {
        // firstWarning = null;
    }

    private ClientPreparedStatement clientPrepareStatement(String sql) throws SQLException {
        ClientPreparedStatement pStmt = null;

        if (this.cachePrepStmts.getValue()) {
            QueryInfo pStmtInfo = this.queryInfoCache.get(sql);

            if (pStmtInfo == null) {
                pStmt = ClientPreparedStatement.getInstance(this, sql);

                this.queryInfoCache.put(sql, pStmt.getQueryInfo());
            } else {
                pStmt = ClientPreparedStatement.getInstance(this, sql, pStmtInfo);
            }
        } else {
            pStmt = ClientPreparedStatement.getInstance(this, sql);
        }
        return pStmt;
    }

    @Override
    public void close() throws SQLException {
        realClose(true, true, false, null);
    }

    @Override
    public void commit() throws SQLException {
        checkClosed();
        try {
            if (this.session.getServerSession().isAutoCommit()) {
                throw SQLError.createSQLException(Messages.getString("Connection.3"));
            }
            this.session.execSQL(null, "COMMIT", null, this.nullStatementResultSetFactory, null);
        } catch (CJException e) {
            throw SQLExceptionsMapping.translateException(e);
        } catch (SQLException sqlException) {
            if (MysqlErrorNumbers.SQL_STATE_COMMUNICATION_LINK_FAILURE.equals(sqlException.getSQLState())) {
                throw SQLError.createSQLException(Messages.getString("Connection.4"), MysqlErrorNumbers.SQL_STATE_TRANSACTION_RESOLUTION_UNKNOWN);
            }
            throw sqlException;
        }
    }

    @Override
    public void createNewIO() throws CJException {
        try {
            connectOneTryOnly();
        } catch (SQLException ex) {
            throw ExceptionFactory.createException(UnableToConnectException.class, ex.getMessage(), ex);
        }
    }

    private void connectOneTryOnly() throws SQLException, CJException {
        Exception connectionNotEstablishedBecause = null;

        try {
            this.session.connect(this.origHostInfo, this.user, this.password, this.database, getLoginTimeout());

            // Server properties might be different from previous connection, so initialize again...
            initializePropsFromServer();

        } catch (UnableToConnectException rejEx) {
            close();
            NativeProtocol protocol = this.session.getProtocol();
            if (protocol != null) {
                protocol.getSocketConnection().forceClose();
            }
            throw rejEx;

        } catch (Exception e) {

            if ((e instanceof PasswordExpiredException
                    || e instanceof SQLException && ((SQLException) e).getErrorCode() == MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD)
                    && !this.disconnectOnExpiredPasswords.getValue()) {
                return;
            }

            if (this.session != null) {
                this.session.forceClose();
            }

            connectionNotEstablishedBecause = e;

            if (e instanceof SQLException) {
                throw (SQLException) e;
            }

            if (e.getCause() != null && e.getCause() instanceof SQLException) {
                throw (SQLException) e.getCause();
            }

            if (e instanceof CJException) {
                throw (CJException) e;
            }

            SQLException chainedEx = SQLError.createSQLException(Messages.getString("Connection.UnableToConnect"),
                    MysqlErrorNumbers.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE);
            chainedEx.initCause(connectionNotEstablishedBecause);

            throw chainedEx;
        }
    }

    private int getLoginTimeout() {
        int loginTimeoutSecs = DriverManager.getLoginTimeout();
        if (loginTimeoutSecs <= 0) {
            return 0;
        }
        return loginTimeoutSecs * 1000;
    }

    private void createPreparedStatementCaches() {
        int cacheSize = this.propertySet.getIntegerProperty(PropertyKey.prepStmtCacheSize).getValue();
        CacheAdapterFactory<String, QueryInfo> cacheFactory = new PerConnectionLRUFactory();
        this.queryInfoCache = cacheFactory.getInstance(this, this.origHostInfo.getDatabaseUrl(), cacheSize, this.prepStmtCacheSqlLimit.getValue());
    }

    @Override
    public java.sql.Statement createStatement() throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public java.sql.Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public java.sql.Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public boolean getAutoCommit() {
        return this.session.getServerSession().isAutoCommit();
    }

    @Override
    public String getCatalog() {
        return this.database;
    }

    @Override
    public int getHoldability() throws SQLException {
        return java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public long getId() {
        return this.session.getThreadId();
    }

    @Override
    public java.sql.DatabaseMetaData getMetaData() throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return this.isolationLevel;
    }

    @Override
    public java.util.Map<String, Class<?>> getTypeMap() throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public Properties getProperties() {
        return this.props;
    }

    /**
     * Sets varying properties that depend on server information. Called once we have connected to the server.
     *
     * @throws SQLException
     *             if a database access error occurs
     */
    private void initializePropsFromServer() throws SQLException, CJException {
        this.session.setSessionVariables();

        this.session.loadServerVariables();

        this.autoIncrementIncrement = this.session.getServerSession().getServerVariable("auto_increment_increment", 1);

        this.session.getProtocol().initServerSession();

        checkTransactionIsolationLevel();

        handleAutoCommitDefaults();
    }

    /**
     * Resets a default auto-commit value of 0 to 1, as required by JDBC specification.
     * Takes into account that the default auto-commit value of 0 may have been changed on the server via init_connect.
     *
     * @throws SQLException
     *             if a database access error occurs
     */
    private void handleAutoCommitDefaults() throws SQLException, CJException {
        boolean resetAutoCommitDefault = false;

        // Server Bug#66884 (SERVER_STATUS is always initiated with SERVER_STATUS_AUTOCOMMIT=1) invalidates "elideSetAutoCommits" feature.
        // TODO Turn this feature back on as soon as the server bug is fixed. Consider making it version specific.
        // if (!getPropertySet().getBooleanReadableProperty(PropertyKey.elideSetAutoCommits).getValue()) {
        String initConnectValue = this.session.getServerSession().getServerVariable("init_connect");
        if (initConnectValue != null && initConnectValue.length() > 0) {
            // auto-commit might have changed

            String s = this.session.queryServerVariable("@@session.autocommit");
            if (s != null) {
                this.session.getServerSession().setAutoCommit(Boolean.parseBoolean(s));
                if (!this.session.getServerSession().isAutoCommit()) {
                    resetAutoCommitDefault = true;
                }
            }

        } else {
            // reset it anyway, the server may have been initialized with --autocommit=0
            resetAutoCommitDefault = true;
        }
        //} else if (getSession().isSetNeededForAutoCommitMode(true)) {
        //    // we're not in standard autocommit=true mode
        //    this.session.setAutoCommit(false);
        //    resetAutoCommitDefault = true;
        //}

        if (resetAutoCommitDefault) {
            try {
                setAutoCommit(true); // required by JDBC spec
            } catch (SQLException ex) {
                if (ex.getErrorCode() != MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD || this.disconnectOnExpiredPasswords.getValue()) {
                    throw ex;
                }
            }
        }
    }

    @Override
    public boolean isClosed() {
        return this.session.isClosed();
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    private int autoIncrementIncrement = 0;

    @Override
    public int getAutoIncrementIncrement() {
        return this.autoIncrementIncrement;
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        if (sql == null) {
            return null;
        }

        try {
            Object escapedSqlResult = EscapeProcessor.escapeSQL(sql, this.getSession().getServerSession().getSessionTimeZone());

            if (escapedSqlResult instanceof String) {
                return (String) escapedSqlResult;
            }

            return ((EscapeProcessorResult) escapedSqlResult).escapedSql;
        } catch (CJException e) {
            throw SQLExceptionsMapping.translateException(e);
        }
    }

    @Override
    public java.sql.CallableStatement prepareCall(String sql) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public java.sql.CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public java.sql.CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public java.sql.PreparedStatement prepareStatement(String sql) throws SQLException {
        checkClosed();

        String nativeSql = nativeSQL(sql);
        return clientPrepareStatement(nativeSql);
    }

    @Override
    public java.sql.PreparedStatement prepareStatement(String sql, int autoGenKeyIndex) throws SQLException {
        java.sql.PreparedStatement pStmt = prepareStatement(sql);
        ((ClientPreparedStatement) pStmt).setRetrieveGeneratedKeys(autoGenKeyIndex == java.sql.Statement.RETURN_GENERATED_KEYS);
        return pStmt;
    }

    @Override
    public java.sql.PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public java.sql.PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public java.sql.PreparedStatement prepareStatement(String sql, int[] autoGenKeyIndexes) throws SQLException {
        java.sql.PreparedStatement pStmt = prepareStatement(sql);
        ((ClientPreparedStatement) pStmt).setRetrieveGeneratedKeys(autoGenKeyIndexes != null && autoGenKeyIndexes.length > 0);
        return pStmt;
    }

    @Override
    public java.sql.PreparedStatement prepareStatement(String sql, String[] autoGenKeyColNames) throws SQLException {
        java.sql.PreparedStatement pStmt = prepareStatement(sql);
        ((ClientPreparedStatement) pStmt).setRetrieveGeneratedKeys(autoGenKeyColNames != null && autoGenKeyColNames.length > 0);
        return pStmt;
    }

    @Override
    public void realClose(boolean calledExplicitly, boolean issueRollback, boolean skipLocalTeardown, Throwable reason) throws SQLException {
        SQLException sqlEx = null;

        if (this.isClosed()) {
            return;
        }

        this.session.setForceClosedReason(reason);

        try {
            if (!skipLocalTeardown) {
                if (!getAutoCommit() && issueRollback) {
                    try {
                        rollback();
                    } catch (SQLException ex) {
                        sqlEx = ex;
                    }
                }

                this.session.quit();
            } else {
                this.session.forceClose();
            }
        } finally {
            this.nullStatementResultSetFactory = null;
        }

        if (sqlEx != null) {
            throw sqlEx;
        }
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public void rollback() throws SQLException {
        checkClosed();
        try {
            if (this.session.getServerSession().isAutoCommit()) {
                throw SQLError.createSQLException(Messages.getString("Connection.20"), MysqlErrorNumbers.SQL_STATE_CONNECTION_NOT_OPEN);
            }
            this.session.execSQL(null, "rollback", null, this.nullStatementResultSetFactory, null);
        } catch (CJException e) {
            throw SQLExceptionsMapping.translateException(e);
        } catch (SQLException sqlException) {
            if (MysqlErrorNumbers.SQL_STATE_COMMUNICATION_LINK_FAILURE.equals(sqlException.getSQLState())) {
                throw SQLError.createSQLException(Messages.getString("Connection.21"), MysqlErrorNumbers.SQL_STATE_TRANSACTION_RESOLUTION_UNKNOWN);
            }
            throw sqlException;
        }
    }

    @Override
    public void rollback(final Savepoint savepoint) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public void setAutoCommit(final boolean autoCommitFlag) throws SQLException {
        checkClosed();

        boolean isAutoCommit = this.session.getServerSession().isAutoCommit();
        try {
            boolean needsSetOnServer;
            if (isAutoCommit == autoCommitFlag) {
                needsSetOnServer = false;
            } else {
                needsSetOnServer = getSession().isSetNeededForAutoCommitMode(autoCommitFlag);
            }

            // this internal value must be set first as failover depends on it being set to true to fail over (which is done by most app servers and
            // connection pools at the end of a transaction), and the driver issues an implicit set based on this value when it (re)-connects to a
            // server so the value holds across connections
            this.session.getServerSession().setAutoCommit(autoCommitFlag);

            if (needsSetOnServer) {
                this.session.execSQL(null, autoCommitFlag ? "SET autocommit=1" : "SET autocommit=0", null, this.nullStatementResultSetFactory, null);
            }
        } catch (CJCommunicationsException e) {
            throw SQLExceptionsMapping.translateException(e);
        } catch (CJException e) {
            // Reset to current autocommit value in case of an exception different than a communication exception occurs.
            this.session.getServerSession().setAutoCommit(isAutoCommit);
            // Update the stacktrace.
            throw SQLError.createSQLException(e.getMessage(), e.getSQLState(), e.getVendorCode(), e.isTransient(), e);
        }
    }

    @Override
    public void setCatalog(final String catalog) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public String getDatabase() throws SQLException {
        return this.database;
    }

    @Override
    public void setHoldability(int arg0) throws SQLException {
        // do nothing
    }

    @Override
    public void setReadOnly(boolean readOnlyFlag) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public java.sql.Savepoint setSavepoint() throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public java.sql.Savepoint setSavepoint(String name) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        checkClosed();
        boolean shouldSendSet = level != this.isolationLevel;
        if (shouldSendSet) {
            String sql = switch (level) {
                case java.sql.Connection.TRANSACTION_NONE -> throw SQLError.createSQLException(Messages.getString("Connection.24"));
                case java.sql.Connection.TRANSACTION_READ_COMMITTED -> "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED";
                case java.sql.Connection.TRANSACTION_READ_UNCOMMITTED -> "SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED";
                case java.sql.Connection.TRANSACTION_REPEATABLE_READ -> "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ";
                case java.sql.Connection.TRANSACTION_SERIALIZABLE -> "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE";
                default -> throw SQLError.createSQLException(Messages.getString("Connection.25", new Object[]{level}), MysqlErrorNumbers.SQL_STATE_DRIVER_NOT_CAPABLE);
            };
            try {
                this.session.execSQL(null, sql, null, this.nullStatementResultSetFactory, null);
            } catch (CJException e) {
                throw SQLExceptionsMapping.translateException(e);
            }
            this.isolationLevel = level;
        }
    }

    @Override
    public void setTypeMap(java.util.Map<String, Class<?>> map) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public String getSchema() {
        return null;
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public void setNetworkTimeout(Executor executor, final int milliseconds) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        try {
            checkClosed();
        } catch (SQLException e) {
            throw SQLExceptionsMapping.translateException(e);
        }
        return this.session.getSocketTimeout();
    }

    @Override
    public Clob createClob() throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public boolean isValid(int timeout) {
        if (isClosed()) {
            return false;
        }

        try {
            this.session.ping(false, timeout * 1000);
        } catch (Throwable t) {
            try {
                abortInternal();
            } catch (Throwable ignoreThrown) {
                // we're dead now anyway
            }

            return false;
        }

        return true;
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        throw new SQLClientInfoException("not supported", Map.of());
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        throw new SQLClientInfoException("not supported", Map.of());
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public java.sql.Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public <T> T unwrap(java.lang.Class<T> iface) throws java.sql.SQLException {
        try {
            // This works for classes that aren't actually wrapping
            // anything
            return iface.cast(this);
        } catch (ClassCastException cce) {
            throw SQLError.createSQLException("Unable to unwrap to " + iface.toString(), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT);
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        // This works for classes that aren't actually wrapping
        // anything
        return iface.isInstance(this);
    }

    @Override
    public NativeSession getSession() {
        return this.session;
    }

    @Override
    public void handleCleanup(Throwable whyCleanedUp) {
        cleanup(whyCleanedUp);
    }
}
