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

public abstract class AbstractProtocol<M extends Message> implements Protocol<M> {

    protected Session session;
    protected SocketConnection socketConnection;

    protected PropertySet propertySet;

    protected AuthenticationProvider<M> authProvider;

    protected MessageBuilder<M> messageBuilder;

    protected PacketSentTimeHolder packetSentTimeHolder;
    protected PacketReceivedTimeHolder packetReceivedTimeHolder;

    @Override
    public void init(Session sess, SocketConnection phConnection, PropertySet propSet) throws CJCommunicationsException {
        this.session = sess;
        this.propertySet = propSet;

        this.socketConnection = phConnection;
    }

    @Override
    public SocketConnection getSocketConnection() {
        return this.socketConnection;
    }

    @Override
    public PropertySet getPropertySet() {
        return this.propertySet;
    }

    @Override
    public void setPropertySet(PropertySet propertySet) {
        this.propertySet = propertySet;
    }

    @Override
    public MessageBuilder<M> getMessageBuilder() {
        return this.messageBuilder;
    }

    @Override
    public void reset() {
        // no-op
    }

}
