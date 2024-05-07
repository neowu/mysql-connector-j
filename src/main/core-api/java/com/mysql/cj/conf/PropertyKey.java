/*
 * Copyright (c) 2018, 2024, Oracle and/or its affiliates.
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

import java.util.Map;
import java.util.TreeMap;

/**
 * PropertyKey handles connection property names, their camel-case aliases and case sensitivity.
 */
public enum PropertyKey {

    /*
     * Properties individually managed after parsing connection string. These property keys are case insensitive.
     */
    /** The database user name. */
    USER("user", false),
    /** The database user password. */
    PASSWORD("password", false),
    /** The hostname value from the properties instance passed to the driver. */
    HOST("host", false),
    /** The port number value from the properties instance passed to the driver. */
    PORT("port", false),
    /** The name pipes path to use when "protocol=pipe'. */
    PATH("path", "namedPipePath", false),
    /** The server type in a replication setup. Possible values: "source" and "replica". */
    TYPE("type", false),
    /** The address value ("host:port") from the properties instance passed to the driver. */
    ADDRESS("address", false),
    /** The host priority in a list of hosts. */
    PRIORITY("priority", false),
    /** The database value from the properties instance passed to the driver. */
    DBNAME("dbname", false), //

    allowMultiQueries("allowMultiQueries", true), //
    allowPublicKeyRetrieval("allowPublicKeyRetrieval", true), //

    blobsAreStrings("blobsAreStrings", true), //

    cachePrepStmts("cachePrepStmts", true), //

    characterEncoding("characterEncoding", true), //

    clientCertificateKeyStorePassword("clientCertificateKeyStorePassword", true), //
    clientCertificateKeyStoreType("clientCertificateKeyStoreType", true), //
    clientCertificateKeyStoreUrl("clientCertificateKeyStoreUrl", true), //

    clobCharacterEncoding("clobCharacterEncoding", true), //
    compensateOnDuplicateKeyUpdateCounts("compensateOnDuplicateKeyUpdateCounts", true), //
    connectionAttributes("connectionAttributes", true), //
    connectionCollation("connectionCollation", true), //

    connectionTimeZone("connectionTimeZone", "serverTimezone", true), //
    connectTimeout("connectTimeout", true), //
    continueBatchOnError("continueBatchOnError", true), //

    disconnectOnExpiredPasswords("disconnectOnExpiredPasswords", true), //
    dontCheckOnDuplicateKeyUpdateInSQL("dontCheckOnDuplicateKeyUpdateInSQL", true), //

    emptyStringsConvertToZero("emptyStringsConvertToZero", true), //

    enableEscapeProcessing("enableEscapeProcessing", true), //

    fallbackToSystemKeyStore("fallbackToSystemKeyStore", true), //
    fallbackToSystemTrustStore("fallbackToSystemTrustStore", true), //
    fipsCompliantJsse("fipsCompliantJsse", true), //

    functionsNeverReturnBlobs("functionsNeverReturnBlobs", true), //

    generateSimpleParameterMetadata("generateSimpleParameterMetadata", true), //

    keyManagerFactoryProvider("KeyManagerFactoryProvider", true), //
    keyStoreProvider("keyStoreProvider", true), //
    largeRowSizeThreshold("largeRowSizeThreshold", true), //

    localSocketAddress("localSocketAddress", true), //

    maxAllowedPacket("maxAllowedPacket", true), //

    password1("password1", true), //
    password2("password2", true), //
    password3("password3", true), //

    prepStmtCacheSize("prepStmtCacheSize", true), //
    prepStmtCacheSqlLimit("prepStmtCacheSqlLimit", true), //

    rewriteBatchedStatements("rewriteBatchedStatements", true), //

    serverConfigCacheFactory("serverConfigCacheFactory", true), //
    serverRSAPublicKeyFile("serverRSAPublicKeyFile", true), //
    sessionVariables("sessionVariables", true), //

    socketTimeout("socketTimeout", true), //

    sslContextProvider("sslContextProvider", true), //
    sslMode("sslMode", true), //

    tcpKeepAlive("tcpKeepAlive", true), //
    tcpNoDelay("tcpNoDelay", true), //
    tcpRcvBuf("tcpRcvBuf", true), //
    tcpSndBuf("tcpSndBuf", true), //
    tcpTrafficClass("tcpTrafficClass", true), //
    tinyInt1isBit("tinyInt1isBit", true), //
    tlsCiphersuites("tlsCiphersuites", "enabledSSLCipherSuites", true), //
    tlsVersions("tlsVersions", "enabledTLSProtocols", true), //

    transformedBitIsBoolean("transformedBitIsBoolean", true), //
    treatMysqlDatetimeAsTimestamp("treatMysqlDatetimeAsTimestamp", true), //

    trustCertificateKeyStorePassword("trustCertificateKeyStorePassword", true), //
    trustCertificateKeyStoreType("trustCertificateKeyStoreType", true), //
    trustCertificateKeyStoreUrl("trustCertificateKeyStoreUrl", true), //
    trustManagerFactoryProvider("trustManagerFactoryProvider", true), //

    useAffectedRows("useAffectedRows", true), //

    useReadAheadInput("useReadAheadInput", true), //

    useUnbufferedInput("useUnbufferedInput", true), //

    yearIsDateType("yearIsDateType", true);

    private String keyName;
    private String ccAlias = null;
    private boolean isCaseSensitive = false;

    private static Map<String, PropertyKey> caseInsensitiveValues = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    static {
        for (PropertyKey pk : values()) {
            if (!pk.isCaseSensitive) {
                caseInsensitiveValues.put(pk.getKeyName(), pk);
                if (pk.getCcAlias() != null) {
                    caseInsensitiveValues.put(pk.getCcAlias(), pk);
                }
            }
        }
    }

    /**
     * Initializes each enum element with the proper key name to be used in the connection string or properties maps.
     *
     * @param keyName
     *            the key name for the enum element.
     * @param isCaseSensitive
     *            is this name case sensitive
     */
    PropertyKey(String keyName, boolean isCaseSensitive) {
        this.keyName = keyName;
        this.isCaseSensitive = isCaseSensitive;
    }

    /**
     * Initializes each enum element with the proper key name to be used in the connection string or properties maps.
     *
     * @param keyName
     *            the key name for the enum element.
     * @param alias
     *            camel-case alias key name
     * @param isCaseSensitive
     *            is this name case sensitive
     */
    PropertyKey(String keyName, String alias, boolean isCaseSensitive) {
        this(keyName, isCaseSensitive);
        this.ccAlias = alias;
    }

    @Override
    public String toString() {
        return this.keyName;
    }

    /**
     * Gets the key name of this enum element.
     *
     * @return
     *         the key name associated with the enum element.
     */
    public String getKeyName() {
        return this.keyName;
    }

    /**
     * Gets the camel-case alias key name of this enum element.
     *
     * @return
     *         the camel-case alias key name associated with the enum element or null.
     */
    public String getCcAlias() {
        return this.ccAlias;
    }

    /**
     * Helper method that normalizes the case of the given key, if it is one of {@link PropertyKey} elements.
     *
     * @param keyName
     *            the key name to normalize.
     * @return
     *         the normalized key name if it belongs to this enum, otherwise returns the input unchanged.
     */
    public static String normalizeCase(String keyName) {
        PropertyKey pk = caseInsensitiveValues.get(keyName);
        return pk == null ? keyName : pk.getKeyName();
        //return keyName;
    }

}
