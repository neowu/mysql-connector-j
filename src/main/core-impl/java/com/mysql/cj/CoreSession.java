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
import com.mysql.cj.protocol.Message;
import com.mysql.cj.protocol.Protocol;
import com.mysql.cj.protocol.ServerSession;

public abstract class CoreSession implements Session {

    protected PropertySet propertySet;

    protected transient Protocol<? extends Message> protocol;

    /** The point in time when this connection was created */
    protected long connectionCreationTimeMillis = 0;
    protected HostInfo hostInfo = null;

    protected RuntimeProperty<String> characterEncoding;
    protected RuntimeProperty<Boolean> disconnectOnExpiredPasswords;

    public CoreSession(HostInfo hostInfo, PropertySet propSet) {
        this.connectionCreationTimeMillis = System.currentTimeMillis();
        this.hostInfo = hostInfo;
        this.propertySet = propSet;

        this.characterEncoding = getPropertySet().getStringProperty(PropertyKey.characterEncoding);
        this.disconnectOnExpiredPasswords = getPropertySet().getBooleanProperty(PropertyKey.disconnectOnExpiredPasswords);
    }

    @Override
    public PropertySet getPropertySet() {
        return this.propertySet;
    }

    @Override
    public HostInfo getHostInfo() {
        return this.hostInfo;
    }

    @Override
    public ServerSession getServerSession() {
        return this.protocol.getServerSession();
    }

    @Override
    public long getThreadId() {
        return this.protocol.getServerSession().getCapabilities().getThreadId();
    }
}
