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

package com.mysql.cj.protocol.a;

import com.mysql.cj.CharsetMapping;
import com.mysql.cj.MessageBuilder;
import com.mysql.cj.Messages;
import com.mysql.cj.MysqlType;
import com.mysql.cj.NativeCharsetSettings;
import com.mysql.cj.NativeSession;
import com.mysql.cj.Query;
import com.mysql.cj.Session;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.conf.RuntimeProperty;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.exceptions.CJConnectionFeatureNotAvailableException;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.CJPacketTooBigException;
import com.mysql.cj.exceptions.ClosedOnExpiredPasswordException;
import com.mysql.cj.exceptions.DataTruncationException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.FeatureNotAvailableException;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.exceptions.PasswordExpiredException;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.jdbc.exceptions.MysqlDataTruncation;
import com.mysql.cj.protocol.AbstractProtocol;
import com.mysql.cj.protocol.ColumnDefinition;
import com.mysql.cj.protocol.ExportControlled;
import com.mysql.cj.protocol.Message;
import com.mysql.cj.protocol.MessageReader;
import com.mysql.cj.protocol.MessageSender;
import com.mysql.cj.protocol.Protocol;
import com.mysql.cj.protocol.ProtocolEntity;
import com.mysql.cj.protocol.ProtocolEntityFactory;
import com.mysql.cj.protocol.ProtocolEntityReader;
import com.mysql.cj.protocol.Resultset;
import com.mysql.cj.protocol.ResultsetRow;
import com.mysql.cj.protocol.ResultsetRows;
import com.mysql.cj.protocol.SocketConnection;
import com.mysql.cj.protocol.ValueEncoder;
import com.mysql.cj.protocol.a.NativeConstants.IntegerDataType;
import com.mysql.cj.protocol.a.NativeConstants.StringLengthDataType;
import com.mysql.cj.protocol.a.NativeConstants.StringSelfDataType;
import com.mysql.cj.protocol.a.result.OkPacket;
import com.mysql.cj.result.IntegerValueFactory;
import com.mysql.cj.result.Row;
import com.mysql.cj.result.StringValueFactory;
import com.mysql.cj.result.ValueFactory;
import com.mysql.cj.util.LazyString;
import com.mysql.cj.util.StringUtils;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.DataTruncation;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.TimeZone;
import java.util.function.Supplier;

public class NativeProtocol extends AbstractProtocol<NativePacketPayload> implements Protocol<NativePacketPayload> {

    protected static final int INITIAL_PACKET_SIZE = 1024;

    protected static final int SSL_REQUEST_LENGTH = 32;

    protected MessageSender<NativePacketPayload> packetSender;
    protected MessageReader<NativePacketHeader, NativePacketPayload> packetReader;

    protected NativeServerSession serverSession;

    //private PacketPayload sendPacket = null;
    protected NativePacketPayload sharedSendPacket = null;
    /** Use this when reading in rows to avoid thousands of new() calls, because the byte arrays just get copied out of the packet anyway */
    protected NativePacketPayload reusablePacket = null;

    protected byte packetSequence = 0;

    private RuntimeProperty<Integer> maxAllowedPacket;

    protected boolean hadWarnings = false;
    private int warningCount = 0;

    protected Map<Class<? extends ProtocolEntity>, ProtocolEntityReader<? extends ProtocolEntity, ? extends Message>> PROTOCOL_ENTITY_CLASS_TO_TEXT_READER;

    static Map<Class<?>, Supplier<ValueEncoder>> DEFAULT_ENCODERS = new HashMap<>(30);
    static {
        DEFAULT_ENCODERS.put(BigDecimal.class, NumberValueEncoder::new);
        DEFAULT_ENCODERS.put(BigInteger.class, NumberValueEncoder::new);

        DEFAULT_ENCODERS.put(Boolean.class, BooleanValueEncoder::new);
        DEFAULT_ENCODERS.put(Byte.class, NumberValueEncoder::new);

        DEFAULT_ENCODERS.put(Double.class, NumberValueEncoder::new);
        DEFAULT_ENCODERS.put(Duration.class, DurationValueEncoder::new);
        DEFAULT_ENCODERS.put(Float.class, NumberValueEncoder::new);

        DEFAULT_ENCODERS.put(Instant.class, InstantValueEncoder::new);
        DEFAULT_ENCODERS.put(Integer.class, NumberValueEncoder::new);
        DEFAULT_ENCODERS.put(LocalDate.class, LocalDateValueEncoder::new);
        DEFAULT_ENCODERS.put(LocalDateTime.class, LocalDateTimeValueEncoder::new);
        DEFAULT_ENCODERS.put(LocalTime.class, LocalTimeValueEncoder::new);
        DEFAULT_ENCODERS.put(Long.class, NumberValueEncoder::new);
        DEFAULT_ENCODERS.put(OffsetDateTime.class, OffsetDateTimeValueEncoder::new);
        DEFAULT_ENCODERS.put(OffsetTime.class, OffsetTimeValueEncoder::new);

        DEFAULT_ENCODERS.put(Short.class, NumberValueEncoder::new);
        DEFAULT_ENCODERS.put(String.class, StringValueEncoder::new);

        DEFAULT_ENCODERS.put(ZonedDateTime.class, ZonedDateTimeValueEncoder::new);
    }

    private NativeMessageBuilder nativeMessageBuilder = null;

    public static NativeProtocol getInstance(Session session, SocketConnection socketConnection, PropertySet propertySet) throws CJCommunicationsException {
        NativeProtocol protocol = new NativeProtocol();
        protocol.init(session, socketConnection, propertySet);
        return protocol;
    }

    public NativeProtocol() {
    }

    @Override
    public void init(Session sess, SocketConnection phConnection, PropertySet propSet) throws CJCommunicationsException {
        super.init(sess, phConnection, propSet);

        this.maxAllowedPacket = this.propertySet.getIntegerProperty(PropertyKey.maxAllowedPacket);

        this.reusablePacket = new NativePacketPayload(INITIAL_PACKET_SIZE);

        try {
            this.packetSender = new SimplePacketSender(this.socketConnection.getMysqlOutput());
            this.packetReader = new SimplePacketReader(this.socketConnection, this.maxAllowedPacket);
        } catch (IOException ioEx) {
            throw ExceptionFactory.createCommunicationsException(this.propertySet, this.serverSession, this.packetSentTimeHolder,
                    this.packetReceivedTimeHolder, ioEx);
        }

        this.authProvider = new NativeAuthenticationProvider();
        this.authProvider.init(this, propertySet);

        Map<Class<? extends ProtocolEntity>, ProtocolEntityReader<? extends ProtocolEntity, NativePacketPayload>> protocolEntityClassToTextReader = new HashMap<>();
        protocolEntityClassToTextReader.put(ColumnDefinition.class, new ColumnDefinitionReader(this));
        protocolEntityClassToTextReader.put(ResultsetRow.class, new ResultsetRowReader(this));
        protocolEntityClassToTextReader.put(Resultset.class, new TextResultsetReader(this));
        this.PROTOCOL_ENTITY_CLASS_TO_TEXT_READER = Map.copyOf(protocolEntityClassToTextReader);
    }

    @Override
    public MessageBuilder<NativePacketPayload> getMessageBuilder() {
        return getNativeMessageBuilder();
    }

    public MessageReader<NativePacketHeader, NativePacketPayload> getPacketReader() {
        return this.packetReader;
    }

    private NativeMessageBuilder getNativeMessageBuilder() {
        if (this.nativeMessageBuilder != null) {
            return this.nativeMessageBuilder;
        }
        return this.nativeMessageBuilder = new NativeMessageBuilder(this.serverSession.supportsQueryAttributes());
    }

    @Override
    public Supplier<ValueEncoder> getValueEncoderSupplier(Object obj) {
        if (obj == null) {
            return NullValueEncoder::new;
        }
        Supplier<ValueEncoder> res = DEFAULT_ENCODERS.get(obj.getClass());
        if (res == null) {
            Optional<Supplier<ValueEncoder>> mysqlType = DEFAULT_ENCODERS.entrySet().stream().filter(m -> m.getKey().isAssignableFrom(obj.getClass()))
                    .map(Entry::getValue).findFirst();
            if (mysqlType.isPresent()) {
                res = mysqlType.get();
            }
        }
        return res;
    }

    /**
     * Negotiates the SSL communication channel used when connecting to a MySQL server that has SSL enabled.
     */
    @Override
    public void negotiateSSLConnection() throws CJException {
        if (!ExportControlled.enabled()) {
            throw new CJConnectionFeatureNotAvailableException(this.getPropertySet(), this.serverSession, this.packetSentTimeHolder, null);
        }

        long clientParam = this.serverSession.getClientParam();

        NativePacketPayload packet = new NativePacketPayload(SSL_REQUEST_LENGTH);
        packet.writeInteger(IntegerDataType.INT4, clientParam);
        packet.writeInteger(IntegerDataType.INT4, NativeConstants.MAX_PACKET_SIZE);
        packet.writeInteger(IntegerDataType.INT1, this.serverSession.getCharsetSettings().configurePreHandshake(false));
        packet.writeBytes(StringLengthDataType.STRING_FIXED, new byte[23]);  // Set of bytes reserved for future use.

        send(packet, packet.getPosition());

        try {
            this.socketConnection.performTlsHandshake(this.serverSession);

            // i/o streams were replaced, build new packet sender/reader
            this.packetSender = new SimplePacketSender(this.socketConnection.getMysqlOutput());
            this.packetReader = new SimplePacketReader(this.socketConnection, this.maxAllowedPacket);

        } catch (FeatureNotAvailableException e) {
            throw new CJConnectionFeatureNotAvailableException(this.getPropertySet(), this.serverSession, this.packetSentTimeHolder, e);
        } catch (IOException e) {
            throw ExceptionFactory.createCommunicationsException(this.propertySet, this.serverSession, this.packetSentTimeHolder, this.packetReceivedTimeHolder, e);
        }
    }

    public void rejectProtocol(NativePacketPayload msg) throws CJException {
        try {
            this.socketConnection.getMysqlSocket().close();
        } catch (Exception e) {
            // ignore
        }

        int errno = 2000;

        NativePacketPayload buf = msg;
        buf.setPosition(1); // skip the packet type
        errno = (int) buf.readInteger(IntegerDataType.INT2);

        String serverErrorMessage = "";
        try {
            serverErrorMessage = buf.readString(StringSelfDataType.STRING_TERM, "ASCII");
        } catch (Exception e) {
            //
        }

        StringBuilder errorBuf = new StringBuilder(Messages.getString("Protocol.0"));
        errorBuf.append(serverErrorMessage);
        errorBuf.append("\"");

        String xOpen = MysqlErrorNumbers.mysqlToSqlState(errno);

        throw ExceptionFactory.createException(MysqlErrorNumbers.get(xOpen) + ", " + errorBuf.toString(), xOpen, errno, false, null);
    }

    @Override
    public void beforeHandshake() throws CJException {
        // Reset packet sequences
        this.packetReader.resetMessageSequence();

        // Create session state
        this.serverSession = new NativeServerSession();

        this.serverSession.setCharsetSettings(new NativeCharsetSettings((NativeSession) this.session));

        // Read the first packet
        this.serverSession.setCapabilities(readServerCapabilities());
    }

    @Override
    public void afterHandshake() throws CJCommunicationsException {
        try {
            applyPacketDecorators(this.packetSender, this.packetReader);
            this.socketConnection.getSocketFactory().afterHandshake();
        } catch (IOException ioEx) {
            throw ExceptionFactory.createCommunicationsException(this.propertySet, this.serverSession, this.packetSentTimeHolder, this.packetReceivedTimeHolder, ioEx);
        }
    }

    /**
     * Apply optional decorators to configured PacketSender and PacketReader.
     *
     * @param sender
     *            {@link MessageSender}
     * @param messageReader
     *            {@link MessageReader}
     */
    public void applyPacketDecorators(MessageSender<NativePacketPayload> sender, MessageReader<NativePacketHeader, NativePacketPayload> messageReader) {
        TimeTrackingPacketSender ttSender = new TimeTrackingPacketSender(sender);
        TimeTrackingPacketReader ttReader = new TimeTrackingPacketReader(new MultiPacketReader(messageReader));

        this.packetReader = ttReader;
        this.packetSender = ttSender;

        this.packetReceivedTimeHolder = ttReader;
        this.packetSentTimeHolder = ttSender;
    }

    @Override
    public NativeCapabilities readServerCapabilities() throws CJException {
        // Read the first packet
        NativePacketPayload buf = readMessage(null);

        // Server Greeting Error packet instead of Server Greeting
        if (buf.isErrorPacket()) {
            rejectProtocol(buf);
        }

        return new NativeCapabilities(buf);
    }

    @Override
    public NativeServerSession getServerSession() {
        return this.serverSession;
    }

    @Override
    public void changeDatabase(String database) throws CJCommunicationsException {
        if (database == null || database.length() == 0) {
            return;
        }

        try {
            sendCommand(getNativeMessageBuilder().buildComInitDb(getSharedSendPacket(), database), false, 0);
        } catch (CJException ex) {
            throw ExceptionFactory.createCommunicationsException(this.getPropertySet(), this.serverSession, this.packetSentTimeHolder, this.packetReceivedTimeHolder, ex);
        }
    }

    @Override
    public final NativePacketPayload readMessage(NativePacketPayload reuse) throws CJException {
        try {
            NativePacketHeader header = this.packetReader.readHeader();
            NativePacketPayload buf = this.packetReader.readMessage(Optional.ofNullable(reuse), header);
            this.packetSequence = header.getMessageSequence();
            return buf;

        } catch (IOException ioEx) {
            throw ExceptionFactory.createCommunicationsException(this.propertySet, this.serverSession, this.packetSentTimeHolder,
                    this.packetReceivedTimeHolder, ioEx);
        } catch (OutOfMemoryError oom) {
            throw ExceptionFactory.createException(oom.getMessage(), MysqlErrorNumbers.SQL_STATE_MEMORY_ALLOCATION_ERROR, 0, false, oom);
        }
    }

    /**
     * @param packet
     *            {@link Message}
     * @param packetLen
     *            length of header + payload
     */
    @Override
    public final void send(Message packet, int packetLen) throws CJException {
        try {
            if (this.maxAllowedPacket.getValue() > 0 && packetLen > this.maxAllowedPacket.getValue()) {
                throw new CJPacketTooBigException(packetLen, this.maxAllowedPacket.getValue());
            }

            this.packetSequence++;
            this.packetSender.send(packet.getByteBuffer(), packetLen, this.packetSequence);

            // Don't hold on to large packets
            if (packet == this.sharedSendPacket) {
                reclaimLargeSharedSendPacket();
            }
        } catch (IOException ioEx) {
            throw ExceptionFactory.createCommunicationsException(this.getPropertySet(), this.serverSession, this.packetSentTimeHolder,
                    this.packetReceivedTimeHolder, ioEx);
        }
    }

    @Override
    public final NativePacketPayload sendCommand(Message queryPacket, boolean skipCheck, int timeoutMillis) throws CJException {
        int command = queryPacket.getByteBuffer()[0];

        this.packetReader.resetMessageSequence();

        int oldTimeout = 0;

        if (timeoutMillis != 0) {
            try {
                oldTimeout = this.socketConnection.getMysqlSocket().getSoTimeout();
                this.socketConnection.getMysqlSocket().setSoTimeout(timeoutMillis);
            } catch (IOException e) {
                throw ExceptionFactory.createCommunicationsException(this.propertySet, this.serverSession, this.packetSentTimeHolder,
                        this.packetReceivedTimeHolder, e);
            }
        }

        try {
            // Clear serverStatus...this value is guarded by an external mutex, as you can only ever be processing one command at a time
            this.serverSession.setStatusFlags(0, true);
            this.hadWarnings = false;
            this.setWarningCount(0);

            try {
                clearInputStream();
                this.packetSequence = -1;
                send(queryPacket, queryPacket.getPosition());

            } catch (CJException ex) {
                // don't wrap CJExceptions
                throw ex;
            } catch (Exception ex) {
                throw ExceptionFactory.createCommunicationsException(this.propertySet, this.serverSession, this.packetSentTimeHolder,
                        this.packetReceivedTimeHolder, ex);
            }

            NativePacketPayload returnPacket = null;

            if (!skipCheck) {
                if (command == NativeConstants.COM_STMT_EXECUTE || command == NativeConstants.COM_STMT_RESET) {
                    this.packetReader.resetMessageSequence();
                }

                returnPacket = checkErrorMessage(command);
            }

            return returnPacket;
        } catch (CJException e) {
            this.serverSession.preserveOldTransactionState();
            throw e;

        } finally {
            if (timeoutMillis != 0) {
                try {
                    this.socketConnection.getMysqlSocket().setSoTimeout(oldTimeout);
                } catch (IOException e) {
                    throw ExceptionFactory.createCommunicationsException(this.propertySet, this.serverSession, this.packetSentTimeHolder,
                            this.packetReceivedTimeHolder, e);
                }
            }
        }
    }

    @Override
    public NativePacketPayload checkErrorMessage() throws CJException {
        return checkErrorMessage(-1);
    }

    /**
     * Checks for errors in the reply packet, and if none, returns the reply
     * packet, ready for reading
     *
     * @param command
     *            the command being issued (if used)
     * @return NativePacketPayload
     * @throws CJException
     *             if an error packet was received
     * @throws CJCommunicationsException
     *             if a database error occurs
     */
    private NativePacketPayload checkErrorMessage(int command) throws CJException {
        NativePacketPayload resultPacket = null;
        this.serverSession.setStatusFlags(0);

        try {
            // Check return value, if we get a java.io.EOFException, the server has gone away. We'll pass it on up the exception chain and let someone higher up
            // decide what to do (barf, reconnect, etc).
            resultPacket = readMessage(this.reusablePacket);
        } catch (CJException ex) {
            // Don't wrap CJExceptions
            throw ex;
        } catch (Exception fallThru) {
            throw ExceptionFactory.createCommunicationsException(this.propertySet, this.serverSession, this.packetSentTimeHolder, this.packetReceivedTimeHolder, fallThru);
        }

        checkErrorMessage(resultPacket);

        return resultPacket;
    }

    public void checkErrorMessage(NativePacketPayload resultPacket) throws CJException {
        resultPacket.setPosition(0);
        byte statusCode = (byte) resultPacket.readInteger(IntegerDataType.INT1);

        // Error handling
        if (statusCode == (byte) 0xff) {
            String serverErrorMessage;
            int errno = 2000;

            errno = (int) resultPacket.readInteger(IntegerDataType.INT2);

            String xOpen = null;

            serverErrorMessage = resultPacket.readString(StringSelfDataType.STRING_TERM, this.serverSession.getCharsetSettings().getErrorMessageEncoding());

            if (serverErrorMessage.charAt(0) == '#') {

                // we have an SQLState
                if (serverErrorMessage.length() > 6) {
                    xOpen = serverErrorMessage.substring(1, 6);
                    serverErrorMessage = serverErrorMessage.substring(6);

                    if (xOpen.equals("HY000")) {
                        xOpen = MysqlErrorNumbers.mysqlToSqlState(errno);
                    }
                } else {
                    xOpen = MysqlErrorNumbers.mysqlToSqlState(errno);
                }
            } else {
                xOpen = MysqlErrorNumbers.mysqlToSqlState(errno);
            }

            clearInputStream();

            StringBuilder errorBuf = new StringBuilder();

            errorBuf.append(serverErrorMessage);

            if (xOpen != null) {
                if (xOpen.startsWith("22")) {
                    throw new DataTruncationException(errorBuf.toString(), 0, true, false, 0, 0, errno);
                }

                if (errno == MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD) {
                    throw ExceptionFactory.createException(PasswordExpiredException.class, errorBuf.toString());

                } else if (errno == MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD_LOGIN) {
                    throw ExceptionFactory.createException(ClosedOnExpiredPasswordException.class, errorBuf.toString());

                } else if (errno == MysqlErrorNumbers.ER_CLIENT_INTERACTION_TIMEOUT) {
                    throw ExceptionFactory.createException(CJCommunicationsException.class, errorBuf.toString(), null);
                }
            }

            throw ExceptionFactory.createException(errorBuf.toString(), xOpen, errno, false, null);

        }
    }

    private void reclaimLargeSharedSendPacket() {
        if (this.sharedSendPacket != null && this.sharedSendPacket.getCapacity() > 1048576) {
            this.sharedSendPacket = new NativePacketPayload(INITIAL_PACKET_SIZE);
        }
    }

    public void clearInputStream() throws CJCommunicationsException {
        try {
            int len;

            // Due to a bug in some older Linux kernels (fixed after the patch "tcp: fix FIONREAD/SIOCINQ"), our SocketInputStream.available() may return 1 even
            // if there is no data in the Stream, so, we need to check if InputStream.skip() actually skipped anything.
            while ((len = this.socketConnection.getMysqlInput().available()) > 0 && this.socketConnection.getMysqlInput().skip(len) > 0) {
                continue;
            }
        } catch (IOException ioEx) {
            throw ExceptionFactory.createCommunicationsException(this.propertySet, this.serverSession, this.packetSentTimeHolder,
                    this.packetReceivedTimeHolder, ioEx);
        }
    }

    /**
     * Don't hold on to overly-large packets
     */
    public void reclaimLargeReusablePacket() {
        if (this.reusablePacket != null && this.reusablePacket.getCapacity() > 1048576) {
            this.reusablePacket = new NativePacketPayload(INITIAL_PACKET_SIZE);
        }
    }

    /**
     * Send a query stored in a packet to the server.
     *
     * @param <T>
     *            extends {@link Resultset}
     * @param callingQuery
     *            {@link Query}
     * @param queryPacket
     *            {@link NativePacketPayload} containing query
     * @param cachedMetadata
     *            use this metadata instead of the one provided on wire
     * @param resultSetFactory
     *            {@link ProtocolEntityFactory}
     * @return T instance
     * @throws IOException
     *             if an i/o error occurs
     */
    public final <T extends Resultset> T sendQueryPacket(Query callingQuery, NativePacketPayload queryPacket, ColumnDefinition cachedMetadata, ProtocolEntityFactory<T, NativePacketPayload> resultSetFactory) throws IOException, CJException, SQLException {

        try {
            // Send query command and sql query string
            NativePacketPayload resultPacket = sendCommand(queryPacket, false, 0);

            T rs = readAllResults(resultPacket, cachedMetadata, resultSetFactory);

            if (this.hadWarnings) {
                scanForAndThrowDataTruncation();
            }

            return rs;

        } catch (CJException sqlEx) {
            if (callingQuery != null) {
                callingQuery.checkCancelTimeout();
            }

            throw sqlEx;

        } finally {
        }
    }

    /**
     * Reads and discards a single MySQL packet.
     *
     * @throws CJException
     *             if the network fails while skipping the
     *             packet.
     */
    public final void skipPacket() throws CJCommunicationsException, CJPacketTooBigException, WrongArgumentException {
        try {
            this.packetReader.skipPacket();
        } catch (IOException ioEx) {
            throw ExceptionFactory.createCommunicationsException(this.propertySet, this.serverSession, this.packetSentTimeHolder, this.packetReceivedTimeHolder, ioEx);
        }
    }

    /**
     * Log-off of the MySQL server and close the socket.
     *
     */
    public final void quit() throws CJException {
        try {
            try {
                if (!ExportControlled.isSSLEstablished(this.socketConnection.getMysqlSocket())) { // Fix for Bug#56979 does not apply to secure sockets.
                    if (!this.socketConnection.getMysqlSocket().isClosed()) {
                        try {
                            // The response won't be read, this fixes BUG#56979 [Improper connection closing logic leads to TIME_WAIT sockets on server].
                            this.socketConnection.getMysqlSocket().shutdownInput();
                        } catch (UnsupportedOperationException e) {
                            // Ignore, some sockets do not support this method.
                        }
                    }
                }
            } catch (IOException e) {
                // Can't do anything constructive about this.
            }

            this.packetSequence = -1;
            NativePacketPayload packet = new NativePacketPayload(1);
            send(getNativeMessageBuilder().buildComQuit(packet), packet.getPosition());
        } finally {
            this.socketConnection.forceClose();
        }
    }

    /**
     * Returns the packet used for sending data (used by PreparedStatement) with position set to 0.
     * Guarded by external synchronization on a mutex.
     *
     * @return A packet to send data with
     */
    public NativePacketPayload getSharedSendPacket() {
        if (this.sharedSendPacket == null) {
            this.sharedSendPacket = new NativePacketPayload(INITIAL_PACKET_SIZE);
        }
        this.sharedSendPacket.setPosition(0);

        return this.sharedSendPacket;
    }

    @Override
    public void connect(String user, String password, String database) throws CJException {
        // session creation & initialization happens here

        beforeHandshake();

        this.authProvider.connect(user, password, database);
    }

    public int getWarningCount() {
        return this.warningCount;
    }

    public void setWarningCount(int warningCount) {
        this.warningCount = warningCount;
    }

    public static MysqlType findMysqlType(PropertySet propertySet, int mysqlTypeId, short colFlag, long length, LazyString tableName,
            LazyString originalTableName, int collationIndex, String encoding) {
        boolean isUnsigned = (colFlag & MysqlType.FIELD_FLAG_UNSIGNED) > 0;
        boolean isFromFunction = originalTableName.length() == 0;
        boolean isBinary = (colFlag & MysqlType.FIELD_FLAG_BINARY) > 0;
        /**
         * Is this field owned by a server-created temporary table?
         */
        boolean isImplicitTemporaryTable = tableName.length() > 0 && tableName.toString().startsWith("#sql_");

        boolean isOpaqueBinary = isBinary && collationIndex == CharsetMapping.MYSQL_COLLATION_INDEX_binary
                && (mysqlTypeId == MysqlType.FIELD_TYPE_STRING || mysqlTypeId == MysqlType.FIELD_TYPE_VAR_STRING || mysqlTypeId == MysqlType.FIELD_TYPE_VARCHAR)
                        ?
                        // queries resolved by temp tables also have this 'signature', check for that
                        !isImplicitTemporaryTable
                        : "binary".equalsIgnoreCase(encoding);

        switch (mysqlTypeId) {
            case MysqlType.FIELD_TYPE_DECIMAL:
            case MysqlType.FIELD_TYPE_NEWDECIMAL:
                return isUnsigned ? MysqlType.DECIMAL_UNSIGNED : MysqlType.DECIMAL;

            case MysqlType.FIELD_TYPE_TINY:
                // Adjust for pseudo-boolean
                if (!isUnsigned && length == 1 && propertySet.getBooleanProperty(PropertyKey.tinyInt1isBit).getValue()) {
                    if (propertySet.getBooleanProperty(PropertyKey.transformedBitIsBoolean).getValue()) {
                        return MysqlType.BOOLEAN;
                    }
                    return MysqlType.BIT;
                }
                return isUnsigned ? MysqlType.TINYINT_UNSIGNED : MysqlType.TINYINT;

            case MysqlType.FIELD_TYPE_SHORT:
                return isUnsigned ? MysqlType.SMALLINT_UNSIGNED : MysqlType.SMALLINT;

            case MysqlType.FIELD_TYPE_LONG:
                return isUnsigned ? MysqlType.INT_UNSIGNED : MysqlType.INT;

            case MysqlType.FIELD_TYPE_FLOAT:
                return isUnsigned ? MysqlType.FLOAT_UNSIGNED : MysqlType.FLOAT;

            case MysqlType.FIELD_TYPE_DOUBLE:
                return isUnsigned ? MysqlType.DOUBLE_UNSIGNED : MysqlType.DOUBLE;

            case MysqlType.FIELD_TYPE_NULL:
                return MysqlType.NULL;

            case MysqlType.FIELD_TYPE_TIMESTAMP:
                return MysqlType.TIMESTAMP;

            case MysqlType.FIELD_TYPE_LONGLONG:
                return isUnsigned ? MysqlType.BIGINT_UNSIGNED : MysqlType.BIGINT;

            case MysqlType.FIELD_TYPE_INT24:
                return isUnsigned ? MysqlType.MEDIUMINT_UNSIGNED : MysqlType.MEDIUMINT;

            case MysqlType.FIELD_TYPE_DATE:
                return MysqlType.DATE;

            case MysqlType.FIELD_TYPE_TIME:
                return MysqlType.TIME;

            case MysqlType.FIELD_TYPE_DATETIME:
                return MysqlType.DATETIME;

            case MysqlType.FIELD_TYPE_YEAR:
                return MysqlType.YEAR;

            case MysqlType.FIELD_TYPE_VARCHAR:
            case MysqlType.FIELD_TYPE_VAR_STRING:

                if (isOpaqueBinary && !(isFromFunction && propertySet.getBooleanProperty(PropertyKey.functionsNeverReturnBlobs).getValue())) {
                    return MysqlType.VARBINARY;
                }

                return MysqlType.VARCHAR;

            case MysqlType.FIELD_TYPE_BIT:
                //if (length > 1) {
                // we need to pretend this is a full binary blob
                //this.colFlag |= MysqlType.FIELD_FLAG_BINARY;
                //this.colFlag |= MysqlType.FIELD_FLAG_BLOB;
                //return MysqlType.VARBINARY;
                //}
                return MysqlType.BIT;

            case MysqlType.FIELD_TYPE_JSON:
                return MysqlType.JSON;

            case MysqlType.FIELD_TYPE_ENUM:
                return MysqlType.ENUM;

            case MysqlType.FIELD_TYPE_SET:
                return MysqlType.SET;

            case MysqlType.FIELD_TYPE_TINY_BLOB:
                if (!isBinary || collationIndex != CharsetMapping.MYSQL_COLLATION_INDEX_binary
                        || propertySet.getBooleanProperty(PropertyKey.blobsAreStrings).getValue()
                        || isFromFunction && propertySet.getBooleanProperty(PropertyKey.functionsNeverReturnBlobs).getValue()) {
                    return MysqlType.TINYTEXT;
                }
                return MysqlType.TINYBLOB;

            case MysqlType.FIELD_TYPE_MEDIUM_BLOB:
                if (!isBinary || collationIndex != CharsetMapping.MYSQL_COLLATION_INDEX_binary
                        || propertySet.getBooleanProperty(PropertyKey.blobsAreStrings).getValue()
                        || isFromFunction && propertySet.getBooleanProperty(PropertyKey.functionsNeverReturnBlobs).getValue()) {
                    return MysqlType.MEDIUMTEXT;
                }
                return MysqlType.MEDIUMBLOB;

            case MysqlType.FIELD_TYPE_LONG_BLOB:
                if (!isBinary || collationIndex != CharsetMapping.MYSQL_COLLATION_INDEX_binary
                        || propertySet.getBooleanProperty(PropertyKey.blobsAreStrings).getValue()
                        || isFromFunction && propertySet.getBooleanProperty(PropertyKey.functionsNeverReturnBlobs).getValue()) {
                    return MysqlType.LONGTEXT;
                }
                return MysqlType.LONGBLOB;

            case MysqlType.FIELD_TYPE_BLOB:
                // Sometimes MySQL uses this protocol-level type for all possible BLOB variants,
                // we can divine what the actual type is by the length reported

                int newMysqlTypeId = mysqlTypeId;

                // fixing initial type according to length
                if (length <= MysqlType.TINYBLOB.getPrecision()) {
                    newMysqlTypeId = MysqlType.FIELD_TYPE_TINY_BLOB;

                } else if (length <= MysqlType.BLOB.getPrecision()) {
                    if (!isBinary || collationIndex != CharsetMapping.MYSQL_COLLATION_INDEX_binary
                            || propertySet.getBooleanProperty(PropertyKey.blobsAreStrings).getValue()
                            || isFromFunction && propertySet.getBooleanProperty(PropertyKey.functionsNeverReturnBlobs).getValue()) {
                        newMysqlTypeId = MysqlType.FIELD_TYPE_VARCHAR;
                        return MysqlType.TEXT;
                    }
                    return MysqlType.BLOB;

                } else if (length <= MysqlType.MEDIUMBLOB.getPrecision()) {
                    newMysqlTypeId = MysqlType.FIELD_TYPE_MEDIUM_BLOB;
                } else {
                    newMysqlTypeId = MysqlType.FIELD_TYPE_LONG_BLOB;
                }

                // call this method again with correct this.mysqlType set
                return findMysqlType(propertySet, newMysqlTypeId, colFlag, length, tableName, originalTableName, collationIndex, encoding);

            case MysqlType.FIELD_TYPE_STRING:
                if (isOpaqueBinary && !propertySet.getBooleanProperty(PropertyKey.blobsAreStrings).getValue()) {
                    return MysqlType.BINARY;
                }
                return MysqlType.CHAR;

            case MysqlType.FIELD_TYPE_GEOMETRY:
                return MysqlType.GEOMETRY;

            case MysqlType.FIELD_TYPE_VECTOR:
                return MysqlType.VECTOR;

            default:
                return MysqlType.UNKNOWN;
        }
    }

    /*
     * Reading results
     */

    @Override
    public <T extends ProtocolEntity> T read(Class<T> requiredClass, ProtocolEntityFactory<T, NativePacketPayload> protocolEntityFactory) throws IOException, CJException {
        @SuppressWarnings("unchecked")
        ProtocolEntityReader<T, NativePacketPayload> sr = (ProtocolEntityReader<T, NativePacketPayload>) this.PROTOCOL_ENTITY_CLASS_TO_TEXT_READER
                .get(requiredClass);
        if (sr == null) {
            throw ExceptionFactory.createException(FeatureNotAvailableException.class, "ProtocolEntityReader isn't available for class " + requiredClass);
        }
        return sr.read(protocolEntityFactory);
    }

    @Override
    public <T extends ProtocolEntity> T read(Class<Resultset> requiredClass, NativePacketPayload resultPacket,
                                             ColumnDefinition metadata, ProtocolEntityFactory<T, NativePacketPayload> protocolEntityFactory) throws IOException, CJException {
        @SuppressWarnings("unchecked")
        ProtocolEntityReader<T, NativePacketPayload> sr = (ProtocolEntityReader<T, NativePacketPayload>) this.PROTOCOL_ENTITY_CLASS_TO_TEXT_READER.get(requiredClass);
        if (sr == null) {
            throw ExceptionFactory.createException(FeatureNotAvailableException.class, "ProtocolEntityReader isn't available for class " + requiredClass);
        }
        return sr.read(resultPacket, metadata, protocolEntityFactory);
    }

    /**
     * Read next result set from multi-result chain.
     *
     * @param <T>
     *            extends {@link ProtocolEntity}
     * @param currentProtocolEntity
     *            T instance
     * @param resultSetFactory
     *            {@link ProtocolEntityFactory}
     * @return T instance
     * @throws IOException
     *             if an i/o error occurs
     */
    public <T extends ProtocolEntity> T readNextResultset(T currentProtocolEntity, ProtocolEntityFactory<T, NativePacketPayload> resultSetFactory) throws IOException, CJException {
        T result = null;
        if (Resultset.class.isAssignableFrom(currentProtocolEntity.getClass()) && this.serverSession.useMultiResults()) {
            if (this.serverSession.hasMoreResults()) {

                T currentResultSet = currentProtocolEntity;
                T newResultSet;
                // we need to consume all result sets which don't contain rows from streamer right now,
                NativePacketPayload fieldPacket = checkErrorMessage();
                fieldPacket.setPosition(0);
                newResultSet = read(Resultset.class, fieldPacket, null, resultSetFactory);
                ((Resultset) currentResultSet).setNextResultset((Resultset) newResultSet);
                currentResultSet = newResultSet;

                if (result == null) {
                    // we should return the first result set in chain
                    result = currentResultSet;
                }
            }
        }
        return result;
    }

    public <T extends Resultset> T readAllResults(NativePacketPayload resultPacket, ColumnDefinition metadata, ProtocolEntityFactory<T, NativePacketPayload> resultSetFactory) throws IOException, CJException, SQLException {
        resultPacket.setPosition(0);
        T topLevelResultSet = read(Resultset.class, resultPacket, metadata, resultSetFactory);

        if (this.serverSession.hasMoreResults()) {
            T currentResultSet = topLevelResultSet;
            while (this.serverSession.hasMoreResults()) {
                currentResultSet = readNextResultset(currentResultSet, resultSetFactory);
            }
            clearInputStream();
        }

        if (this.hadWarnings) {
            scanForAndThrowDataTruncation();
        }

        reclaimLargeReusablePacket();
        return topLevelResultSet;
    }

    @SuppressWarnings("unchecked")
    public final <T> T readServerStatusForResultSets(NativePacketPayload rowPacket, boolean saveOldStatus) throws WrongArgumentException {
        T result = null;
        if (rowPacket.isEOFPacket()) {
            // read EOF packet
            rowPacket.setPosition(1); // skip the packet signature header
            this.warningCount = (int) rowPacket.readInteger(IntegerDataType.INT2);
            if (this.warningCount > 0) {
                this.hadWarnings = true; // this is a 'latch', it's reset by sendCommand()
            }

            this.serverSession.setStatusFlags((int) rowPacket.readInteger(IntegerDataType.INT2), saveOldStatus);

        } else {
            // read OK packet
            OkPacket ok = OkPacket.parse(rowPacket, this.serverSession);
            result = (T) ok;

            this.serverSession.setStatusFlags(ok.getStatusFlags(), saveOldStatus);

            this.warningCount = ok.getWarningCount();
            if (this.warningCount > 0) {
                this.hadWarnings = true; // this is a 'latch', it's reset by sendCommand()
            }
        }
        return result;
    }

    public void scanForAndThrowDataTruncation() throws CJException, SQLException {
        // jdbcCompliantTruncation will be set to false on com.mysql.cj.jdbc.ConnectionImpl#setupServerForTruncationChecks
        // for as original behavior, technically, it never reports data truncation warnings, e.g. BigDecimal with excessive scales
//        if (this.propertySet.getBooleanProperty(PropertyKey.jdbcCompliantTruncation).getValue() && getWarningCount() > 0) {
//            int warningCountOld = getWarningCount();
//            convertShowWarningsToSQLWarnings(true);
//            setWarningCount(warningCountOld);
//        }
    }

    /**
     * Turns output of 'SHOW WARNINGS' into JDBC SQLWarning instances.
     *
     * If 'forTruncationOnly' is true, only looks for truncation warnings, and
     * actually throws DataTruncation as an exception.
     *
     * @param forTruncationOnly
     *            if this method should only scan for data truncation warnings
     *
     * @return the SQLWarning chain (or null if no warnings)
     */
    public SQLWarning convertShowWarningsToSQLWarnings(boolean forTruncationOnly) throws CJException, SQLException {
        if (this.warningCount == 0) {
            return null;
        }

        SQLWarning currentWarning = null;
        ResultsetRows rows = null;

        try {
            NativePacketPayload resultPacket = sendCommand(getNativeMessageBuilder().buildComQuery(getSharedSendPacket(), "SHOW WARNINGS"), false, 0);

            Resultset warnRs = readAllResults(resultPacket, null,
                    new ResultsetFactory());

            int codeFieldIndex = warnRs.getColumnDefinition().findColumn("Code", 1) - 1;
            int messageFieldIndex = warnRs.getColumnDefinition().findColumn("Message", 1) - 1;

            ValueFactory<String> svf = new StringValueFactory(this.propertySet);
            ValueFactory<Integer> ivf = new IntegerValueFactory(this.propertySet);

            rows = warnRs.getRows();
            Row r;
            while ((r = rows.next()) != null) {

                int code = r.getValue(codeFieldIndex, ivf);

                if (forTruncationOnly) {
                    if (code == MysqlErrorNumbers.ER_WARN_DATA_TRUNCATED || code == MysqlErrorNumbers.ER_WARN_DATA_OUT_OF_RANGE) {
                        DataTruncation newTruncation = new MysqlDataTruncation(r.getValue(messageFieldIndex, svf), 0, false, false, 0, 0, code);

                        if (currentWarning == null) {
                            currentWarning = newTruncation;
                        } else {
                            currentWarning.setNextWarning(newTruncation);
                        }
                    }
                } else {
                    //String level = warnRs.getString("Level");
                    String message = r.getValue(messageFieldIndex, svf);

                    SQLWarning newWarning = new SQLWarning(message, MysqlErrorNumbers.mysqlToSqlState(code), code);
                    if (currentWarning == null) {
                        currentWarning = newWarning;
                    } else {
                        currentWarning.setNextWarning(newWarning);
                    }
                }
            }

            if (forTruncationOnly && currentWarning != null) {
                throw ExceptionFactory.createException(currentWarning.getMessage(), currentWarning);
            }

            return currentWarning;
        } catch (IOException ex) {
            throw ExceptionFactory.createException(ex.getMessage(), ex);
        }
    }

    /**
     * Configures the client's timezone if required.
     *
     * @throws CJException
     *             if the timezone the server is configured to use can't be
     *             mapped to a Java timezone.
     */
    @Override
    public void configureTimeZone() {
        String connectionTimeZone = getPropertySet().getStringProperty(PropertyKey.connectionTimeZone).getValue();

        TimeZone selectedTz;
        if (connectionTimeZone == null || StringUtils.isEmptyOrWhitespaceOnly(connectionTimeZone) || "LOCAL".equals(connectionTimeZone)) {
            selectedTz = TimeZone.getDefault();
        } else if ("SERVER".equals(connectionTimeZone)) {
            // Session time zone will be detected after the first ServerSession.getSessionTimeZone() call.
            return;
        } else {
            selectedTz = TimeZone.getTimeZone(ZoneId.of(connectionTimeZone)); // TODO use ZoneId.of(String zoneId, Map<String, String> aliasMap) for custom abbreviations support
        }

        this.serverSession.setSessionTimeZone(selectedTz);
    }

    @Override
    public void initServerSession() throws CJException {
        configureTimeZone();

        if (this.serverSession.getServerVariables().containsKey("max_allowed_packet")) {
            int serverMaxAllowedPacket = this.serverSession.getServerVariable("max_allowed_packet", -1);

            // use server value if maxAllowedPacket hasn't been given, or max_allowed_packet is smaller
            if (serverMaxAllowedPacket != -1 && (!this.maxAllowedPacket.isExplicitlySet() || serverMaxAllowedPacket < this.maxAllowedPacket.getValue())) {
                this.maxAllowedPacket.setValue(serverMaxAllowedPacket);
            }
        }

        this.serverSession.getCharsetSettings().configurePostHandshake(false);
    }

}
