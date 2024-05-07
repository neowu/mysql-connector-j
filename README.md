# MySQL Connector/J
patched for cloud env and core-ng framework

```kotlin
maven {
    url = uri("https://neowu.github.io/maven-repo/")
    content {
        includeGroupByRegex("core\\.framework.*")
    }
}

dependencies {
    runtime("core.framework.mysql:mysql-connector-j:8.4.0-r2")
}
```

# Changes
* removed all synchronization to be virtual thread friendly, connection/statement/resultSet are not thread safe anymore
* added google cloud auth support for cancel query timer
* simplify code used by core-ng framework

# Removed features
* PropertyKey.cacheServerConfiguration: only loaded once when connect to mysql
* AbandonedConnectionCleanupThread
* PropertyKey.useServerPrepStmts
* PropertyKey.useCursorFetch

* PropertyKey.autoSlowLog
* PropertyKey.logSlowQueries
* PropertyKey.slowQueryThresholdMillis
* PropertyKey.profileSQL
* PropertyKey.explainSlowQueries
* PropertyKey.profilerEventHandler 
* PropertyKey.maxQuerySizeToLog
* PropertyKey.dumpQueriesOnException
* PropertyKey.useUsageAdvisor
* PropertyKey.resultSetSizeThreshold
* PropertyKey.emulateUnsupportedPstmts
* ProfilerEventHandler
* AuthenticationLdapSaslClientPlugin, AuthenticationKerberosClient

* PropertyKey.gatherPerfMetrics

* PropertyKey.cacheResultSetMetadata
* PropertyKey.metadataCacheSize

* SqlTimeValueFactory, SqlTimestampValueFactory, UtilCalendarValueFactory, SqlDateValueFactory
* removed synchronization from PerConnectionLRUFactory
* UpdatableResultSet

* PropertyKey.dontTrackOpenResources
* PropertyKey.holdResultsOpenOverStatementClose
* CallableStatement
* PropertyKey.preserveInstants (always use as true)
* InputStream/Reader/Blob/Clob/SQLXML/byte[] (not use streaming and binary encoding)
* ExceptionInterceptors
* QueryAttribute
* PropertyKey.allowLoadLocalInfile
* DataSource / XA
* PropertyKey.pedantic
* streaming mode and binary encoding
* connectionLifecycleInterceptors
* MetaData
* PropertyKey.paranoid
* PropertyKey.trackSessionState
* ClientInfoProvider
* auto-reconnect
* Savepoint
* PING_MARKER
* Property.createDatabaseIfNotExist
* PropertyKey.useCompression
* PropertyKey.interactiveClient
* old JDBC behavior flags, 
* "LOAD DATA LOCAL INFILE"
* PropertyKey.useConfigs
* PropertyKey.alwaysSendSetIsolation / PropertyKey.useLocalSessionState (only minor impact on connectionImpl, use false and true behavior, not as default)
* PropertyKey.useLocalTransactionState
* PropertyKey.ignoreNonTxTables
* PropertyKey.useOnlyServerErrorMessages
* PropertyKey.zeroDateTimeBehavior
* PropertyKey.includeInnodbStatusInDeadlockExceptions/includeThreadDumpInDeadlockExceptions/includeThreadNamesAsStatementComment/autoGenerateTestcaseScript (not useful in microservice arch)
* internal query execution time tracking
* simplify String encoding, remove NCHAR/NVARCHAR, use VARCHAR w/ utf8mb4 instead
* PropertyKey.cacheDefaultTimeZone
* PropertyKey.forceConnectionTimeZoneToSession
* PropertyKey.databaseTerm
* added QueryDiagnostic, removed QueryInterceptor
* removed Load balancing and replication support
* not allowed sql_mode: ANSI_QUOTES, NO_BACKSLASH_ESCAPES, TIME_TRUNCATE_FRACTIONAL
* simplified character encoding handling, removed PropertyKey.characterSetResults / PropertyKey.passwordCharacterEncoding
* simplified serverTruncatesFractionalSecond

* Statement methods (in favor of ClientPreparedStatement)
* PropertyKey.processEscapeCodesForPrepStmts / PropertyKey.scrollTolerantForwardOnly

* PropertyKey.propertiesTransform
* Connection Proxy / dnsSrv / HA
* Connection read only
* PropertyKey.padCharsWithSpace

* PropertyKey.socksProxyHost/socksProxyPort/socksProxyRemoteDns
* PropertyKey.jdbcCompliantTruncation
* PropertyKey.socketFactory (always use TCP protocol and StandardSocketFactory)
* PropertyKey.authenticationPlugins/disabledAuthenticationPlugins/defaultAuthenticationPlugin (replace with default values)

* OpenTelemetry
* PropertyKey.queryInfoCacheFactory (always use PerConnectionLRUFactory)
* PropertyKey.allowNanAndInf

* PropertyKey.enableQueryTimeouts

# TODO
remove com.mysql.cj.jdbc.JdbcStatement.removeOpenResultSet
remove SessionEventListener?
support cloud iam auth thru HostInfo natively?
remove WrongArgumentException?
remove com.mysql.cj.jdbc.result.ResultSetImpl.prev and all non-forward-only methods? 
