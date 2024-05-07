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
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.protocol.Protocol;
import com.mysql.cj.protocol.ServerSession;

/**
 * {@link Session} exposes logical level which user API uses internally to call {@link Protocol} methods.
 * It's a higher-level abstraction than MySQL server session ({@link ServerSession}). {@link Protocol} and {@link ServerSession} methods
 * should never be used directly from user API.
 *
 */
public interface Session {

    PropertySet getPropertySet();

    /**
     * Log-off of the MySQL server and close the socket.
     *
     */
    void quit();

    /**
     * Clobbers the physical network connection and marks this session as closed.
     */
    void forceClose();

    long getThreadId();

    boolean isSetNeededForAutoCommitMode(boolean autoCommitFlag);

    HostInfo getHostInfo();

    ServerSession getServerSession();

    boolean isSSLEstablished();

    /**
     * Add listener for this session status changes.
     *
     * @param l
     *            {@link SessionEventListener} instance.
     */
    void addListener(SessionEventListener l);

    interface SessionEventListener {
        void handleCleanup(Throwable whyCleanedUp);
    }

    boolean isClosed();
}
