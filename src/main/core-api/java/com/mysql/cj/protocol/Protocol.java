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

package com.mysql.cj.protocol;

import com.mysql.cj.MessageBuilder;
import com.mysql.cj.Session;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.exceptions.CJException;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * A protocol provides the facilities to communicate with a MySQL server.
 *
 * @param <M>
 *            Message type
 */
public interface Protocol<M extends Message> {

    /**
     * Init method takes the place of constructor.
     *
     * A constructor should be used unless the encapsulation of ProtocolFactory is necessary.
     *
     * @param session
     *            {@link Session}
     * @param socketConnection
     *            {@link SocketConnection}
     * @param propertySet
     *            {@link PropertySet}
     */
    void init(Session session, SocketConnection socketConnection, PropertySet propertySet) throws CJCommunicationsException;

    PropertySet getPropertySet();

    void setPropertySet(PropertySet propertySet);

    MessageBuilder<M> getMessageBuilder();

    /**
     * Retrieve ServerCapabilities from server.
     *
     * @return {@link ServerCapabilities}
     */
    ServerCapabilities readServerCapabilities() throws CJException;

    ServerSession getServerSession();

    SocketConnection getSocketConnection();

    /**
     * Create a new session. This generally happens once at the beginning of a connection.
     *
     * @param user
     *            DB user name
     * @param password
     *            DB user password
     * @param database
     *            database name
     */
    void connect(String user, String password, String database) throws CJException;

    void negotiateSSLConnection() throws CJException;

    void beforeHandshake() throws CJException;

    void afterHandshake() throws CJCommunicationsException;

    void changeDatabase(String database) throws CJCommunicationsException;

    /**
     * Read one message from the MySQL server into the reusable buffer if provided or into the new one.
     *
     * @param reuse
     *            {@link Message} instance to read into, may be null
     * @return the message from the server.
     */
    M readMessage(M reuse) throws CJException;

    /**
     * Read one message from the MySQL server, checks for errors in it, and if none,
     * returns the message, ready for reading
     *
     * @return a message ready for reading.
     */
    M checkErrorMessage() throws CJException;

    /**
     * @param message
     *            {@link Message} instance
     * @param packetLen
     *            length of header + payload
     */
    void send(Message message, int packetLen) throws CJException;

    /**
     * Send a command to the MySQL server.
     *
     * @param queryPacket
     *            a packet pre-loaded with data for the protocol (eg.
     *            from a client-side prepared statement). The first byte of
     *            this packet is the MySQL protocol 'command' from MysqlDefs
     * @param skipCheck
     *            do not call checkErrorPacket() if true
     * @param timeoutMillis
     *            timeout
     *
     * @return the response packet from the server
     *
     * @throws CJException
     *             if an I/O error or SQL error occurs
     */

    M sendCommand(Message queryPacket, boolean skipCheck, int timeoutMillis) throws CJException;

    <T extends ProtocolEntity> T read(Class<T> requiredClass, ProtocolEntityFactory<T, M> protocolEntityFactory) throws IOException, CJException;

    /**
     * Read protocol entity.
     *
     * @param requiredClass
     *            required Resultset class
     * @param resultPacket
     *            the first packet of information in the result set
     * @param metadata
     *            use this metadata instead of the one provided on wire
     * @param protocolEntityFactory
     *            {@link ProtocolEntityFactory} instance
     * @param <T>
     *            object extending the {@link ProtocolEntity}
     * @return
     *         {@link ProtocolEntity} instance
     * @throws IOException
     *             if an error occurs
     */
    <T extends ProtocolEntity> T read(Class<Resultset> requiredClass, M resultPacket, ColumnDefinition metadata, ProtocolEntityFactory<T, M> protocolEntityFactory) throws IOException, CJException;

    void configureTimeZone();

    void initServerSession() throws CJException;

    /**
     * Return Protocol to its initial state right after successful connect.
     */
    void reset();

    Supplier<ValueEncoder> getValueEncoderSupplier(Object obj);

}
