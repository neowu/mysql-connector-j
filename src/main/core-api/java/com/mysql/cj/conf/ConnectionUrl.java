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

package com.mysql.cj.conf;

import com.mysql.cj.Messages;
import com.mysql.cj.conf.url.SingleConnectionUrl;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.UnsupportedConnectionStringException;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.util.LRUCache;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static com.mysql.cj.util.StringUtils.isNullOrEmpty;

/**
 * A container for a database URL and a collection of given connection arguments.
 * The connection string is parsed and split by its components, each of which is then processed and fixed according to the needs of the connection type.
 * This abstract class holds all common behavior to all connection string types. Its subclasses must implement their own specifics such as classifying hosts by
 * type or apply validation rules.
 */
public abstract class ConnectionUrl implements DatabaseUrlContainer {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 3306;

    private static final LRUCache<String, ConnectionUrl> connectionUrlCache = new LRUCache<>(100);
    private static final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    /**
     * The database URL type which is determined by the scheme section of the connection string.
     */
    public enum Type {
        // Standard schemes:
        SINGLE_CONNECTION("jdbc:mysql:");

        private final String scheme;

        private Type(String scheme) {
            this.scheme = scheme;
        }

        public String getScheme() {
            return this.scheme;
        }

        /**
         * Checks if the given scheme corresponds to one of the connection types the driver supports.
         *
         * @param scheme
         *            scheme part from connection string, like "jdbc:mysql:"
         * @return true if the given scheme is supported by driver
         */
        public static boolean isSupported(String scheme) {
            for (Type t : values()) {
                if (t.getScheme().equalsIgnoreCase(scheme)) {
                    return true;
                }
            }
            return false;
        }

    }

    protected Type type;
    protected String originalConnStr;
    protected String originalDatabase;
    protected List<HostInfo> hosts = new ArrayList<>();
    protected Map<String, String> properties = new HashMap<>();

    /**
     * Static factory method that returns either a new instance of a {@link ConnectionUrl} or a cached one.
     * Returns "null" it can't handle the connection string.
     *
     * @param connString
     *            the connection string
     * @param info
     *            the connection arguments map
     * @return an instance of a {@link ConnectionUrl} or "null" if isn't able to handle the connection string
     */
    public static ConnectionUrl getConnectionUrlInstance(String connString, Properties info) throws SQLException, UnsupportedConnectionStringException, WrongArgumentException {
        if (connString == null) {
            throw new SQLException(Messages.getString("ConnectionString.0"));
        }
        String connStringCacheKey = buildConnectionStringCacheKey(connString, info);
        ConnectionUrl connectionUrl;

        rwLock.readLock().lock();
        connectionUrl = connectionUrlCache.get(connStringCacheKey);
        if (connectionUrl == null) {
            rwLock.readLock().unlock();
            rwLock.writeLock().lock();
            try {
                // Check again, in the meantime it could have been cached by another thread.
                connectionUrl = connectionUrlCache.get(connStringCacheKey);
                if (connectionUrl == null) {
                    ConnectionUrlParser connStrParser = ConnectionUrlParser.parseConnectionString(connString);
                    connectionUrl = new SingleConnectionUrl(connStrParser, info);
                    connectionUrlCache.put(connStringCacheKey, connectionUrl);
                }
                rwLock.readLock().lock();
            } finally {
                rwLock.writeLock().unlock();
            }
        }
        rwLock.readLock().unlock();
        return connectionUrl;
    }

    /**
     * Builds a connection URL cache map key based on the connection string itself plus the string representation of the given connection properties.
     *
     * @param connString
     *            the connection string
     * @param info
     *            the connection arguments map
     * @return a connection string cache map key
     */
    private static String buildConnectionStringCacheKey(String connString, Properties info) {
        StringBuilder sbKey = new StringBuilder(connString);
        sbKey.append("\u00A7"); // Section sign.
        sbKey.append(
                info == null ? null : info.stringPropertyNames().stream().map(k -> k + "=" + info.getProperty(k)).collect(Collectors.joining(", ", "{", "}")));
        return sbKey.toString();
    }

    /**
     * Checks if this {@link ConnectionUrl} is able to process the given database URL.
     *
     * @param connString
     *            the connection string
     * @return true if this class is able to process the given URL, false otherwise
     */
    public static boolean acceptsUrl(String connString) throws WrongArgumentException {
        return ConnectionUrlParser.isConnectionStringSupported(connString);
    }

    /**
     * Constructs an instance of {@link ConnectionUrl}, performing all the required initializations.
     *
     * @param connStrParser
     *            a {@link ConnectionUrlParser} instance containing the parsed version of the original connection string
     * @param info
     *            the connection arguments map
     */
    protected ConnectionUrl(ConnectionUrlParser connStrParser, Properties info) throws WrongArgumentException {
        this.originalConnStr = connStrParser.getDatabaseUrl();
        this.originalDatabase = connStrParser.getPath() == null ? "" : connStrParser.getPath();
        collectProperties(connStrParser, info); // Fill properties before filling hosts info.
        collectHostsInfo(connStrParser);
    }

    /**
     * Joins the connection arguments from the connection string with the ones from the given connection arguments map collecting them in a single map.
     * Additionally may also collect other connection arguments from configuration files.
     *
     * @param connStrParser
     *            the {@link ConnectionUrlParser} from where to collect the properties
     * @param info
     *            the connection arguments map
     */
    protected void collectProperties(ConnectionUrlParser connStrParser, Properties info) throws WrongArgumentException {
        // Fill in the properties from the connection string.
        connStrParser.getProperties().entrySet().stream().forEach(e -> this.properties.put(PropertyKey.normalizeCase(e.getKey()), e.getValue()));

        // Properties passed in override the ones from the connection string.
        if (info != null) {
            info.stringPropertyNames().stream().forEach(k -> this.properties.put(PropertyKey.normalizeCase(k), info.getProperty(k)));
        }
    }

    /**
     * Collects the hosts information from the {@link ConnectionUrlParser}.
     *
     * @param connStrParser
     *            the {@link ConnectionUrlParser} from where to collect the hosts information
     */
    protected void collectHostsInfo(ConnectionUrlParser connStrParser) throws WrongArgumentException {
        List<HostInfo> hostInfos = this.hosts;
        for (HostInfo hostInfo : connStrParser.getHosts()) {
            HostInfo info = fixHostInfo(hostInfo);
            hostInfos.add(info);
        }
    }

    /**
     * Fixes the host information by moving data around and filling in missing data.
     *
     * @param hi
     *            the host information data to fix
     * @return a new {@link HostInfo} with all required data
     */
    protected HostInfo fixHostInfo(HostInfo hi) throws WrongArgumentException {
        Map<String, String> hostProps = new HashMap<>();

        // Add global connection arguments.
        hostProps.putAll(this.properties);
        // Add/override host specific connection arguments.
        hi.getHostProperties().entrySet().stream().forEach(e -> hostProps.put(PropertyKey.normalizeCase(e.getKey()), e.getValue()));
        // Add the database name.
        if (!hostProps.containsKey(PropertyKey.DBNAME.getKeyName())) {
            hostProps.put(PropertyKey.DBNAME.getKeyName(), getDatabase());
        }

        String host = hostProps.remove(PropertyKey.HOST.getKeyName());
        if (!isNullOrEmpty(hi.getHost())) {
            host = hi.getHost();
        } else if (isNullOrEmpty(host)) {
            host = getDefaultHost();
        }

        String portAsString = hostProps.remove(PropertyKey.PORT.getKeyName());
        int port = hi.getPort();
        if (port == HostInfo.NO_PORT && !isNullOrEmpty(portAsString)) {
            try {
                port = Integer.parseInt(portAsString);
            } catch (NumberFormatException e) {
                throw ExceptionFactory.createException(WrongArgumentException.class,
                        Messages.getString("ConnectionString.7", new Object[] { hostProps.get(PropertyKey.PORT.getKeyName()) }), e);
            }
        }
        if (port == HostInfo.NO_PORT) {
            port = getDefaultPort();
        }

        String user = hostProps.remove(PropertyKey.USER.getKeyName());
        if (!isNullOrEmpty(hi.getUser())) {
            user = hi.getUser();
        } else if (isNullOrEmpty(user)) {
            user = getDefaultUser();
        }

        String password = hostProps.remove(PropertyKey.PASSWORD.getKeyName());
        if (hi.getPassword() != null) { // Password can be specified as empty string.
            password = hi.getPassword();
        } else if (isNullOrEmpty(password)) {
            password = getDefaultPassword();
        }

        return new HostInfo(this, host, port, user, password, hostProps);
    }

    /**
     * Returns the default host. Subclasses must override this method if they have different default host value.
     *
     * @return the default host
     */
    public String getDefaultHost() {
        return DEFAULT_HOST;
    }

    /**
     * Returns the default port. Subclasses must override this method if they have different default port value.
     *
     * @return the default port
     */
    public int getDefaultPort() {
        return DEFAULT_PORT;
    }

    /**
     * Returns the default user. Usually the one provided in the method {@link DriverManager#getConnection(String, String, String)} or as connection argument.
     *
     * @return the default user
     */
    public String getDefaultUser() {
        return this.properties.get(PropertyKey.USER.getKeyName());
    }

    /**
     * Returns the default password. Usually the one provided in the method {@link DriverManager#getConnection(String, String, String)} or as connection
     * argument.
     *
     * @return the default password
     */
    public String getDefaultPassword() {
        return this.properties.get(PropertyKey.PASSWORD.getKeyName());
    }

    /**
     * Returns this connection URL type.
     *
     * @return the connection URL type
     */
    public Type getType() {
        return this.type;
    }

    /**
     * Returns the original database URL that produced this connection string.
     *
     * @return the original database URL
     */
    @Override
    public String getDatabaseUrl() {
        return this.originalConnStr;
    }

    /**
     * Returns the database from this connection URL. Note that a "DBNAME" property overrides the database identified in the connection string.
     *
     * @return the database name
     */
    public String getDatabase() {
        return this.properties.containsKey(PropertyKey.DBNAME.getKeyName()) ? this.properties.get(PropertyKey.DBNAME.getKeyName()) : this.originalDatabase;
    }

    /**
     * Returns the single or first host info structure.
     *
     * @return the first host info structure
     */
    public HostInfo getMainHost() {
        return this.hosts.isEmpty() ? null : this.hosts.getFirst();
    }

    /**
     * Returns a string representation of this object.
     *
     * @return a string representation of this object
     */
    @Override
    public String toString() {
        StringBuilder asStr = new StringBuilder(super.toString());
        asStr.append(String.format(" :: {type: \"%s\", hosts: %s, database: \"%s\", properties: %s}", this.type, this.hosts, this.originalDatabase, this.properties));
        return asStr.toString();
    }

}
