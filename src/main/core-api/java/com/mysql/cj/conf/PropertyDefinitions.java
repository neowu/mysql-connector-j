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

package com.mysql.cj.conf;

import com.mysql.cj.Messages;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class PropertyDefinitions {
    /*
     * Built-in system properties.
     */
    public static final String SYSP_java_vendor = "java.vendor";
    public static final String SYSP_java_version = "java.version";
    public static final String SYSP_os_name = "os.name";
    public static final String SYSP_os_arch = "os.arch";

    /*
     * Categories of connection properties.
     */
    public static final String CATEGORY_AUTH = Messages.getString("ConnectionProperties.categoryAuthentication");
    public static final String CATEGORY_CONNECTION = Messages.getString("ConnectionProperties.categoryConnection");
    public static final String CATEGORY_SESSION = Messages.getString("ConnectionProperties.categorySession");
    public static final String CATEGORY_NETWORK = Messages.getString("ConnectionProperties.categoryNetworking");
    public static final String CATEGORY_SECURITY = Messages.getString("ConnectionProperties.categorySecurity");
    public static final String CATEGORY_STATEMENTS = Messages.getString("ConnectionProperties.categoryStatements");
    public static final String CATEGORY_PREPARED_STATEMENTS = Messages.getString("ConnectionProperties.categoryPreparedStatements");
    public static final String CATEGORY_RESULT_SETS = Messages.getString("ConnectionProperties.categoryResultSets");
    public static final String CATEGORY_BLOBS = Messages.getString("ConnectionProperties.categoryBlobs");
    public static final String CATEGORY_DATETIMES = Messages.getString("ConnectionProperties.categoryDatetimes");
    public static final String CATEGORY_PERFORMANCE = Messages.getString("ConnectionProperties.categoryPerformance");
    public static final String CATEGORY_XDEVAPI = Messages.getString("ConnectionProperties.categoryXDevAPI");
    public static final String CATEGORY_USER_DEFINED = Messages.getString("ConnectionProperties.categoryUserDefined");

    /*
     * Property modifiers.
     */
    public static final boolean DEFAULT_VALUE_TRUE = true;
    public static final boolean DEFAULT_VALUE_FALSE = false;
    public static final String DEFAULT_VALUE_NULL_STRING = null;

    /** is modifiable in run-time */
    public static final boolean RUNTIME_MODIFIABLE = true;

    /** is not modifiable in run-time (will allow to set not-null value only once) */
    public static final boolean RUNTIME_NOT_MODIFIABLE = false;

    public enum SslMode {
        PREFERRED, REQUIRED, VERIFY_CA, VERIFY_IDENTITY, DISABLED;
    }

    /**
     * Static unmodifiable {@link PropertyKey} -&gt; {@link PropertyDefinition} map.
     */
    public static final Map<PropertyKey, PropertyDefinition<?>> PROPERTY_KEY_TO_PROPERTY_DEFINITION;

    static {
        PropertyDefinition<?>[] pdefs = new PropertyDefinition<?>[] {
                //
                // CATEGORY_AUTHENTICATION
                //
                new StringPropertyDefinition(PropertyKey.USER, DEFAULT_VALUE_NULL_STRING, RUNTIME_NOT_MODIFIABLE,
                        Messages.getString("ConnectionProperties.Username"), Messages.getString("ConnectionProperties.allVersions"), CATEGORY_AUTH,
                        Integer.MIN_VALUE + 1),

                new StringPropertyDefinition(PropertyKey.PASSWORD, DEFAULT_VALUE_NULL_STRING, RUNTIME_NOT_MODIFIABLE,
                        Messages.getString("ConnectionProperties.Password"), Messages.getString("ConnectionProperties.allVersions"), CATEGORY_AUTH,
                        Integer.MIN_VALUE + 2),

                new StringPropertyDefinition(PropertyKey.password1, DEFAULT_VALUE_NULL_STRING, RUNTIME_NOT_MODIFIABLE,
                        Messages.getString("ConnectionProperties.Password1"), "8.0.28", CATEGORY_AUTH, Integer.MIN_VALUE + 3),

                new StringPropertyDefinition(PropertyKey.password2, DEFAULT_VALUE_NULL_STRING, RUNTIME_NOT_MODIFIABLE,
                        Messages.getString("ConnectionProperties.Password2"), "8.0.28", CATEGORY_AUTH, Integer.MIN_VALUE + 4),

                new StringPropertyDefinition(PropertyKey.password3, DEFAULT_VALUE_NULL_STRING, RUNTIME_NOT_MODIFIABLE,
                        Messages.getString("ConnectionProperties.Password3"), "8.0.28", CATEGORY_AUTH, Integer.MIN_VALUE + 5),

                //
                // CATEGORY_CONNECTION
                //
                new StringPropertyDefinition(PropertyKey.connectionAttributes, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.connectionAttributes"), "5.1.25", CATEGORY_CONNECTION, 7),

                new BooleanPropertyDefinition(PropertyKey.useAffectedRows, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useAffectedRows"), "5.1.7", CATEGORY_CONNECTION, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.disconnectOnExpiredPasswords, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.disconnectOnExpiredPasswords"), "5.1.23", CATEGORY_CONNECTION, Integer.MIN_VALUE),

                //
                // CATEGORY_SESSION
                //
                new StringPropertyDefinition(PropertyKey.characterEncoding, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.characterEncoding"), "1.1g", CATEGORY_SESSION, Integer.MIN_VALUE),

                new StringPropertyDefinition(PropertyKey.connectionCollation, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.connectionCollation"), "3.0.13", CATEGORY_SESSION, Integer.MIN_VALUE),

                new StringPropertyDefinition(PropertyKey.sessionVariables, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.sessionVariables"), "3.1.8", CATEGORY_SESSION, Integer.MAX_VALUE),

                //
                // CATEGORY_NETWORK
                //
                new BooleanPropertyDefinition(PropertyKey.useUnbufferedInput, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useUnbufferedInput"), "3.0.11", CATEGORY_NETWORK, Integer.MIN_VALUE),

                new IntegerPropertyDefinition(PropertyKey.connectTimeout, 0, RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.connectTimeout"),
                        "3.0.1", CATEGORY_NETWORK, 9, 0, Integer.MAX_VALUE),

                new StringPropertyDefinition(PropertyKey.localSocketAddress, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.localSocketAddress"), "5.0.5", CATEGORY_NETWORK, Integer.MIN_VALUE),

                new IntegerPropertyDefinition(PropertyKey.socketTimeout, 0, RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.socketTimeout"),
                        "3.0.1", CATEGORY_NETWORK, 10, 0, Integer.MAX_VALUE),

                new BooleanPropertyDefinition(PropertyKey.tcpNoDelay, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.tcpNoDelay"), "5.0.7", CATEGORY_NETWORK, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.tcpKeepAlive, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.tcpKeepAlive"), "5.0.7", CATEGORY_NETWORK, Integer.MIN_VALUE),

                new IntegerPropertyDefinition(PropertyKey.tcpRcvBuf, 0, RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.tcpSoRcvBuf"), "5.0.7",
                        CATEGORY_NETWORK, Integer.MIN_VALUE, 0, Integer.MAX_VALUE),

                new IntegerPropertyDefinition(PropertyKey.tcpSndBuf, 0, RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.tcpSoSndBuf"), "5.0.7",
                        CATEGORY_NETWORK, Integer.MIN_VALUE, 0, Integer.MAX_VALUE),

                new IntegerPropertyDefinition(PropertyKey.tcpTrafficClass, 0, RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.tcpTrafficClass"),
                        "5.0.7", CATEGORY_NETWORK, Integer.MIN_VALUE, 0, 255),

                new IntegerPropertyDefinition(PropertyKey.maxAllowedPacket, 65535, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.maxAllowedPacket"), "5.1.8", CATEGORY_NETWORK, Integer.MIN_VALUE),

                //
                // CATEGORY_SECURITY
                //

                new StringPropertyDefinition(PropertyKey.serverRSAPublicKeyFile, DEFAULT_VALUE_NULL_STRING, RUNTIME_NOT_MODIFIABLE,
                        Messages.getString("ConnectionProperties.serverRSAPublicKeyFile"), "5.1.31", CATEGORY_SECURITY, 2),

                new BooleanPropertyDefinition(PropertyKey.allowPublicKeyRetrieval, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.allowPublicKeyRetrieval"), "5.1.31", CATEGORY_SECURITY, 3),

                new EnumPropertyDefinition<>(PropertyKey.sslMode, SslMode.PREFERRED, RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.sslMode"),
                        CATEGORY_SECURITY),

                new StringPropertyDefinition(PropertyKey.trustCertificateKeyStoreUrl, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.trustCertificateKeyStoreUrl"), "5.1.0", CATEGORY_SECURITY, 5),

                new StringPropertyDefinition(PropertyKey.trustCertificateKeyStoreType, "JKS", RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.trustCertificateKeyStoreType"), "5.1.0", CATEGORY_SECURITY, 6),

                new StringPropertyDefinition(PropertyKey.trustCertificateKeyStorePassword, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.trustCertificateKeyStorePassword"), "5.1.0", CATEGORY_SECURITY, 7),

                new BooleanPropertyDefinition(PropertyKey.fallbackToSystemTrustStore, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.fallbackToSystemTrustStore"), "8.0.22", CATEGORY_SECURITY, 8),

                new StringPropertyDefinition(PropertyKey.clientCertificateKeyStoreUrl, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.clientCertificateKeyStoreUrl"), "5.1.0", CATEGORY_SECURITY, 9),

                new StringPropertyDefinition(PropertyKey.clientCertificateKeyStoreType, "JKS", RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.clientCertificateKeyStoreType"), "5.1.0", CATEGORY_SECURITY, 10),

                new StringPropertyDefinition(PropertyKey.clientCertificateKeyStorePassword, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.clientCertificateKeyStorePassword"), "5.1.0", CATEGORY_SECURITY, 11),

                new BooleanPropertyDefinition(PropertyKey.fallbackToSystemKeyStore, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.fallbackToSystemKeyStore"), "8.0.22", CATEGORY_SECURITY, 12),

                new StringPropertyDefinition(PropertyKey.tlsCiphersuites, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.tlsCiphersuites"), "5.1.35", CATEGORY_SECURITY, 13),

                new StringPropertyDefinition(PropertyKey.tlsVersions, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.tlsVersions"), "8.0.8", CATEGORY_SECURITY, 14),

                new BooleanPropertyDefinition(PropertyKey.fipsCompliantJsse, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.fipsCompliantJsse"), "8.1.0", CATEGORY_SECURITY, 15),

                new StringPropertyDefinition(PropertyKey.keyManagerFactoryProvider, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.keyManagerFactoryProvider"), "8.1.0", CATEGORY_SECURITY, 16),

                new StringPropertyDefinition(PropertyKey.trustManagerFactoryProvider, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.trustManagerFactoryProvider"), "8.1.0", CATEGORY_SECURITY, 17),

                new StringPropertyDefinition(PropertyKey.keyStoreProvider, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.keyStoreProvider"), "8.1.0", CATEGORY_SECURITY, 18),

                new StringPropertyDefinition(PropertyKey.sslContextProvider, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.sslContextProvider"), "8.1.0", CATEGORY_SECURITY, 19),

                new BooleanPropertyDefinition(PropertyKey.allowMultiQueries, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.allowMultiQueries"), "3.1.1", CATEGORY_SECURITY, Integer.MAX_VALUE),

                //
                // CATEGORY_STATEMENTS
                //
                new BooleanPropertyDefinition(PropertyKey.continueBatchOnError, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.continueBatchOnError"), "3.0.3", CATEGORY_STATEMENTS, Integer.MIN_VALUE),

                //
                // CATEGORY_PREPARED_STATEMENTS
                //
                new BooleanPropertyDefinition(PropertyKey.compensateOnDuplicateKeyUpdateCounts, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.compensateOnDuplicateKeyUpdateCounts"), "5.1.7", CATEGORY_PREPARED_STATEMENTS,
                        Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.generateSimpleParameterMetadata, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.generateSimpleParameterMetadata"), "5.0.5", CATEGORY_PREPARED_STATEMENTS, Integer.MIN_VALUE),

                //
                // CATEGORY_RESULT_SETS
                //
                new BooleanPropertyDefinition(PropertyKey.emptyStringsConvertToZero, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.emptyStringsConvertToZero"), "3.1.8", CATEGORY_RESULT_SETS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.tinyInt1isBit, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.tinyInt1isBit"), "3.0.16", CATEGORY_RESULT_SETS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.transformedBitIsBoolean, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.transformedBitIsBoolean"), "3.1.9", CATEGORY_RESULT_SETS, Integer.MIN_VALUE),

                //
                // CATEGORY_BLOBS
                //
                new BooleanPropertyDefinition(PropertyKey.blobsAreStrings, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.blobsAreStrings"), "5.0.8", CATEGORY_BLOBS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.functionsNeverReturnBlobs, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.functionsNeverReturnBlobs"), "5.0.8", CATEGORY_BLOBS, Integer.MIN_VALUE),

                new StringPropertyDefinition(PropertyKey.clobCharacterEncoding, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.clobCharacterEncoding"), "5.0.0", CATEGORY_BLOBS, Integer.MIN_VALUE),

                //
                // CATEGORY_DATETIMES
                //
                new StringPropertyDefinition(PropertyKey.connectionTimeZone, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.connectionTimeZone"), "3.0.2", CATEGORY_DATETIMES, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.treatMysqlDatetimeAsTimestamp, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.treatMysqlDatetimeAsTimestamp"), "8.2.0", CATEGORY_DATETIMES, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.yearIsDateType, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.yearIsDateType"), "3.1.9", CATEGORY_DATETIMES, Integer.MIN_VALUE),

                //
                // CATEGORY_PERFORMANCE
                //

                new BooleanPropertyDefinition(PropertyKey.cachePrepStmts, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.cachePrepStmts"), "3.0.10", CATEGORY_PERFORMANCE, Integer.MIN_VALUE),

                new MemorySizePropertyDefinition(PropertyKey.largeRowSizeThreshold, 2048, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.largeRowSizeThreshold"), "5.1.1", CATEGORY_PERFORMANCE, Integer.MIN_VALUE, 0,
                        Integer.MAX_VALUE),

                new IntegerPropertyDefinition(PropertyKey.prepStmtCacheSize, 25, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.prepStmtCacheSize"), "3.0.10", CATEGORY_PERFORMANCE, 10, 0, Integer.MAX_VALUE),

                new IntegerPropertyDefinition(PropertyKey.prepStmtCacheSqlLimit, 256, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.prepStmtCacheSqlLimit"), "3.0.10", CATEGORY_PERFORMANCE, 11, 1, Integer.MAX_VALUE),

                new BooleanPropertyDefinition(PropertyKey.rewriteBatchedStatements, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.rewriteBatchedStatements"), "3.1.13", CATEGORY_PERFORMANCE, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.useReadAheadInput, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useReadAheadInput"), "3.1.5", CATEGORY_PERFORMANCE, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.dontCheckOnDuplicateKeyUpdateInSQL, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.dontCheckOnDuplicateKeyUpdateInSQL"), "5.1.32", CATEGORY_PERFORMANCE, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PropertyKey.enableEscapeProcessing, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.enableEscapeProcessing"), "6.0.1", CATEGORY_PERFORMANCE, Integer.MIN_VALUE),
        };

        HashMap<PropertyKey, PropertyDefinition<?>> propertyKeyToPropertyDefinitionMap = new HashMap<>();
        for (PropertyDefinition<?> pdef : pdefs) {
            propertyKeyToPropertyDefinitionMap.put(pdef.getPropertyKey(), pdef);
        }
        PROPERTY_KEY_TO_PROPERTY_DEFINITION = Collections.unmodifiableMap(propertyKeyToPropertyDefinitionMap);
    }
}
