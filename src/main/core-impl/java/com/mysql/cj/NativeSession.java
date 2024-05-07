/*
 * Copyright (c) 2015, 2024, Oracle and/or its affiliates.
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

import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.conf.RuntimeProperty;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.ConnectionIsClosedException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.OperationCancelledException;
import com.mysql.cj.protocol.ColumnDefinition;
import com.mysql.cj.protocol.ProtocolEntityFactory;
import com.mysql.cj.protocol.Resultset;
import com.mysql.cj.protocol.SocketConnection;
import com.mysql.cj.protocol.a.NativeMessageBuilder;
import com.mysql.cj.protocol.a.NativePacketPayload;
import com.mysql.cj.protocol.a.NativeProtocol;
import com.mysql.cj.protocol.a.NativeServerSession;
import com.mysql.cj.protocol.a.NativeSocketConnection;
import com.mysql.cj.protocol.a.ResultsetFactory;
import com.mysql.cj.result.Field;
import com.mysql.cj.result.Row;
import com.mysql.cj.result.StringValueFactory;
import com.mysql.cj.result.ValueFactory;
import com.mysql.cj.util.StringUtils;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class NativeSession extends CoreSession {
    private NativeMessageBuilder commandBuilder = null;

    /** Has this session been closed? */
    private boolean isClosed = true;

    /** Why was this session implicitly closed, if known? (for diagnostics) */
    private Throwable forceClosedReason;

    private CopyOnWriteArrayList<WeakReference<SessionEventListener>> listeners = new CopyOnWriteArrayList<>();

    public NativeSession(HostInfo hostInfo, PropertySet propSet) {
        super(hostInfo, propSet);
    }

    public void connect(HostInfo hi, String user, String password, String database, int loginTimeout) throws IOException, CJException {
        this.hostInfo = hi;

        // TODO do we need different types of physical connections?
        SocketConnection socketConnection = new NativeSocketConnection();
        socketConnection.connect(this.hostInfo.getHost(), this.hostInfo.getPort(), this.propertySet, loginTimeout);

        // we use physical connection to create a -> protocol
        // this configuration places no knowledge of protocol or session on physical connection.
        // physical connection is responsible *only* for I/O streams
        if (this.protocol == null) {
            this.protocol = NativeProtocol.getInstance(this, socketConnection, this.propertySet);
        } else {
            this.protocol.init(this, socketConnection, this.propertySet);
        }

        // use protocol to create a -> session
        // protocol is responsible for building a session and authenticating (using AuthenticationProvider) internally
        this.protocol.connect(user, password, database);

        this.isClosed = false;

        this.commandBuilder = new NativeMessageBuilder(this.getServerSession().supportsQueryAttributes());
    }

    // TODO: this method should not be used in user-level APIs
    public NativeProtocol getProtocol() {
        return (NativeProtocol) this.protocol;
    }

    @Override
    public void quit() {
        if (this.protocol != null) {
            try {
                ((NativeProtocol) this.protocol).quit();
            } catch (Exception e) {
            }

        }
        this.isClosed = true;
    }

    // TODO: we should examine the call flow here, we shouldn't have to know about the socket connection but this should be address in a wider scope.
    @Override
    public void forceClose() {
        if (this.protocol != null) {
            // checking this.protocol != null isn't enough if connection is used concurrently (the usual situation
            // with application servers which have additional thread management), this.protocol can become null
            // at any moment after this check, causing a race condition and NPEs on next calls;
            // but we may ignore them because at this stage null this.protocol means that we successfully closed all resources by other thread.
            try {
                this.protocol.getSocketConnection().forceClose();
            } catch (Throwable t) {
                // can't do anything about it, and we're forcibly aborting
            }
            //this.protocol = null; // TODO actually we shouldn't remove protocol instance because some it's methods can be called after closing socket
        }
        this.isClosed = true;
    }

    public void enableMultiQueries() throws CJException {
        this.protocol.sendCommand(this.commandBuilder.buildComSetOption(((NativeProtocol) this.protocol).getSharedSendPacket(), 0), false, 0);
        // OK_PACKET returned in previous sendCommand() was not processed so keep original transaction state.
        ((NativeServerSession) getServerSession()).preserveOldTransactionState();
    }

    public void disableMultiQueries() throws CJException {
        this.protocol.sendCommand(this.commandBuilder.buildComSetOption(((NativeProtocol) this.protocol).getSharedSendPacket(), 1), false, 0);
        // OK_PACKET returned in previous sendCommand() was not processed so keep original transaction state.
        ((NativeServerSession) getServerSession()).preserveOldTransactionState();
    }

    @Override
    public boolean isSetNeededForAutoCommitMode(boolean autoCommitFlag) {
        // Server Bug#66884 (SERVER_STATUS is always initiated with SERVER_STATUS_AUTOCOMMIT=1) invalidates "elideSetAutoCommits" feature.
        // TODO Turn this feature back on as soon as the server bug is fixed. Consider making it version specific.
        //return this.protocol.getServerSession().isSetNeededForAutoCommitMode(autoCommitFlag,
        //        getPropertySet().getBooleanReadableProperty(PropertyKey.elideSetAutoCommits).getValue());
        return ((NativeServerSession) this.protocol.getServerSession()).isSetNeededForAutoCommitMode(autoCommitFlag, false);
    }

    public int getSocketTimeout() {
        RuntimeProperty<Integer> sto = getPropertySet().getProperty(PropertyKey.socketTimeout);
        return sto.getValue();
    }

    /**
     * Returns the packet used for sending data (used by PreparedStatement) with position set to 0.
     * Guarded by external synchronization on a mutex.
     *
     * @return A packet to send data with
     */
    public NativePacketPayload getSharedSendPacket() {
        return ((NativeProtocol) this.protocol).getSharedSendPacket();
    }

    @Override
    public boolean isSSLEstablished() {
        return this.protocol.getSocketConnection().isSSLEstablished();
    }

    /**
     * Loads the result of 'SHOW VARIABLES' into the serverVariables field so
     * that the driver can configure itself.
     */
    public void loadServerVariables() throws CJException, SQLException {
        try {
            var variables = new HashMap<String, String>();
            String queryBuf = "SELECT" +
                              "  @@session.auto_increment_increment AS auto_increment_increment" +
                              ", @@character_set_client AS character_set_client" +
                              ", @@character_set_connection AS character_set_connection" +
                              ", @@character_set_results AS character_set_results" +
                              ", @@character_set_server AS character_set_server" +
                              ", @@collation_server AS collation_server" +
                              ", @@collation_connection AS collation_connection" +
                              ", @@init_connect AS init_connect" +
                              ", @@lower_case_table_names AS lower_case_table_names" +
                              ", @@max_allowed_packet AS max_allowed_packet" +
                              ", @@net_write_timeout AS net_write_timeout" +
                              ", @@performance_schema AS performance_schema" +
                              ", @@sql_mode AS sql_mode" +
                              ", @@system_time_zone AS system_time_zone" +
                              ", @@time_zone AS time_zone" +
                              ", @@transaction_isolation AS transaction_isolation" +
                              ", @@wait_timeout AS wait_timeout";
            NativePacketPayload resultPacket = (NativePacketPayload) this.protocol.sendCommand(this.commandBuilder.buildComQuery(null, queryBuf),
                    false, 0);
            Resultset rs = ((NativeProtocol) this.protocol).readAllResults(resultPacket, null,
                    new ResultsetFactory());
            Field[] f = rs.getColumnDefinition().getFields();
            if (f.length > 0) {
                ValueFactory<String> vf = new StringValueFactory(this.propertySet);
                Row r;
                if ((r = rs.getRows().next()) != null) {
                    for (int i = 0; i < f.length; i++) {
                        String value = r.getValue(i, vf);
                        variables.put(f[i].getColumnLabel(), value);
                    }
                }
            }

            String sqlMode = variables.get("sql_mode");
            if (sqlMode.contains("NO_BACKSLASH_ESCAPES")) throw new Error("sql_mode NO_BACKSLASH_ESCAPES is not supported");
            if (sqlMode.contains("ANSI_QUOTES")) throw new Error("sql_mode ANSI_QUOTES is not supported");
            if (sqlMode.contains("TIME_TRUNCATE_FRACTIONAL")) throw new Error("sql_mode TIME_TRUNCATE_FRACTIONAL is not supported");
            if (!sqlMode.contains("STRICT_TRANS_TABLES")) throw new Error("sql_mode must have STRICT_TRANS_TABLES");

            this.protocol.getServerSession().setServerVariables(variables);
        } catch (IOException e) {
            throw ExceptionFactory.createException(e.getMessage(), e);
        }
    }

    public void setSessionVariables() throws CJException {
        String sessionVariables = getPropertySet().getStringProperty(PropertyKey.sessionVariables).getValue();
        if (sessionVariables != null) {
            List<String> variablesToSet = new ArrayList<>();
            for (String part : StringUtils.split(sessionVariables, ",", "\"'(", "\"')", "\"'", true)) {
                variablesToSet.addAll(StringUtils.split(part, ";", "\"'(", "\"')", "\"'", true));
            }

            if (!variablesToSet.isEmpty()) {
                StringBuilder query = new StringBuilder("SET ");
                String separator = "";
                for (String variableToSet : variablesToSet) {
                    if (variableToSet.length() > 0) {
                        query.append(separator);
                        if (!variableToSet.startsWith("@")) {
                            query.append("SESSION ");
                        }
                        query.append(variableToSet);
                        separator = ",";
                    }
                }
                this.protocol.sendCommand(this.commandBuilder.buildComQuery(null, query.toString()), false, 0);
            }
        }
    }

    /**
     * Get the variable value from server.
     *
     * @param varName
     *            server variable name
     * @return server variable value
     */
    public String queryServerVariable(String varName) throws CJException, SQLException {
        try {

            NativePacketPayload resultPacket = (NativePacketPayload) this.protocol.sendCommand(this.commandBuilder.buildComQuery(null, "SELECT " + varName),
                    false, 0);
            Resultset rs = ((NativeProtocol) this.protocol).readAllResults(resultPacket, null, new ResultsetFactory());

            ValueFactory<String> svf = new StringValueFactory(this.propertySet);
            Row r;
            if ((r = rs.getRows().next()) != null) {
                String s = r.getValue(0, svf);
                if (s != null) {
                    return s;
                }
            }

            return null;

        } catch (IOException e) {
            throw ExceptionFactory.createException(e.getMessage(), e);
        }
    }

    /**
     * Send a query to the server. Returns one of the ResultSet objects.
     * To ensure that Statement's queries are serialized, calls to this method
     * should be enclosed in a connection mutex synchronized block.
     *
     * @param <T>
     *            extends {@link Resultset}
     * @param callingQuery
     *            {@link Query} object
     * @param query
     *            the SQL statement to be executed
     * @param packet
     *            {@link NativePacketPayload}
     * @param resultSetFactory
     *            {@link ProtocolEntityFactory}
     * @param cachedMetadata
     *            use this metadata instead of the one provided on wire
     * @return a ResultSet holding the results
     */
    public <T extends Resultset> T execSQL(Query callingQuery, String query, NativePacketPayload packet, ProtocolEntityFactory<T, NativePacketPayload> resultSetFactory, ColumnDefinition cachedMetadata) throws CJException {
        try {
            if (packet == null) {
                packet = this.commandBuilder.buildComQuery(null, this, query, callingQuery, this.characterEncoding.getValue());
            }
            return ((NativeProtocol) this.protocol).sendQueryPacket(callingQuery, packet, cachedMetadata, resultSetFactory);

        } catch (CJException sqlE) {
            if (sqlE instanceof CJCommunicationsException) {
                invokeCleanupListeners(sqlE);
            }
            throw sqlE;

        } catch (Throwable ex) {
            throw ExceptionFactory.createException(ex.getMessage(), ex);
        }
    }

    public void ping(boolean checkForClosedConnection, int timeoutMillis) throws CJException {
        if (checkForClosedConnection) {
            checkClosed();
        }
        this.protocol.sendCommand(this.commandBuilder.buildComPing(null), false, timeoutMillis); // it isn't safe to use a shared packet here
    }

    @Override
    public boolean isClosed() {
        return this.isClosed;
    }

    public void checkClosed() throws ConnectionIsClosedException, OperationCancelledException {
        if (this.isClosed) {
            if (this.forceClosedReason != null && this.forceClosedReason.getClass().equals(OperationCancelledException.class)) {
                throw (OperationCancelledException) this.forceClosedReason;
            }
            throw ExceptionFactory.createException(ConnectionIsClosedException.class, Messages.getString("Connection.2"), this.forceClosedReason);
        }
    }

    public void setForceClosedReason(Throwable forceClosedReason) {
        this.forceClosedReason = forceClosedReason;
    }

    @Override
    public void addListener(SessionEventListener l) {
        this.listeners.addIfAbsent(new WeakReference<>(l));
    }

    public void invokeCleanupListeners(Throwable whyCleanedUp) {
        for (WeakReference<SessionEventListener> wr : this.listeners) {
            SessionEventListener l = wr.get();
            if (l != null) {
                l.handleCleanup(whyCleanedUp);
            } else {
                this.listeners.remove(wr);
            }
        }
    }
}
