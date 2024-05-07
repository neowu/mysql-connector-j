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

package com.mysql.cj.exceptions;

import com.mysql.cj.Messages;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.protocol.PacketReceivedTimeHolder;
import com.mysql.cj.protocol.PacketSentTimeHolder;
import com.mysql.cj.protocol.ServerSession;

import java.net.BindException;
import java.net.NetworkInterface;
import java.net.SocketException;

public class ExceptionFactory {

    private static final long DEFAULT_WAIT_TIMEOUT_SECONDS = 28800;

    private static final int DUE_TO_TIMEOUT_FALSE = 0;

    private static final int DUE_TO_TIMEOUT_MAYBE = 2;

    private static final int DUE_TO_TIMEOUT_TRUE = 1;

    public static CJException createException(String message) {
        return createException(CJException.class, message);
    }

    @SuppressWarnings("unchecked")
    public static <T extends CJException> T createException(Class<T> clazz, String message) {
        T sqlEx;
        try {
            sqlEx = clazz.getConstructor(String.class).newInstance(message);
        } catch (Throwable e) {
            sqlEx = (T) new CJException(message);
        }
        return sqlEx;
    }

    public static CJException createException(String message, Throwable cause) {
        return createException(CJException.class, message, cause);
    }

    public static <T extends CJException> T createException(Class<T> clazz, String message, Throwable cause) {
        T sqlEx = createException(clazz, message);
        if (cause != null) {
            try {
                sqlEx.initCause(cause);
            } catch (Throwable t) {
                // we're not going to muck with that here, since it's an error condition anyway!
            }

            if (cause instanceof CJException) {
                sqlEx.setSQLState(((CJException) cause).getSQLState());
                sqlEx.setVendorCode(((CJException) cause).getVendorCode());
                sqlEx.setTransient(((CJException) cause).isTransient());
            }
        }
        return sqlEx;
    }

    public static CJException createException(String message, String sqlState, int vendorErrorCode, boolean isTransient, Throwable cause) {
        CJException ex = createException(CJException.class, message, cause);
        ex.setSQLState(sqlState);
        ex.setVendorCode(vendorErrorCode);
        ex.setTransient(isTransient);
        return ex;
    }

    public static CJCommunicationsException createCommunicationsException(PropertySet propertySet, ServerSession serverSession,
            PacketSentTimeHolder packetSentTimeHolder, PacketReceivedTimeHolder packetReceivedTimeHolder, Throwable cause) {
        CJCommunicationsException sqlEx = createException(CJCommunicationsException.class, null, cause);
        sqlEx.init(propertySet, serverSession, packetSentTimeHolder, packetReceivedTimeHolder);
        return sqlEx;
    }

    /**
     * Creates a communications link failure message to be used in CommunicationsException
     * that (hopefully) has some better information and suggestions based on heuristics.
     *
     * @param propertySet
     *            property set
     * @param serverSession
     *            server session
     * @param sentTime
     *            sentTime
     * @param receivedTime
     *            receivedTime
     * @param underlyingException
     *            underlyingException
     * @return message
     */
    public static String createLinkFailureMessageBasedOnHeuristics(PropertySet propertySet, ServerSession serverSession,
            PacketSentTimeHolder sentTime, PacketReceivedTimeHolder receivedTime, Throwable underlyingException) {
        long serverTimeoutSeconds = 0;

        long lastPacketReceivedTimeMs = receivedTime == null ? 0L : receivedTime.getLastPacketReceivedTime();
        long lastPacketSentTimeMs = sentTime == null ? 0L: sentTime.getLastPacketSentTime();
        if (lastPacketSentTimeMs > lastPacketReceivedTimeMs) {
            lastPacketSentTimeMs = sentTime == null ? 0L: sentTime.getPreviousPacketSentTime();
        }

        if (propertySet != null) {
            String serverTimeoutSecondsStr = null;

            if (serverSession != null) {
                serverTimeoutSecondsStr = serverSession.getServerVariable("wait_timeout");
            }

            if (serverTimeoutSecondsStr != null) {
                try {
                    serverTimeoutSeconds = Long.parseLong(serverTimeoutSecondsStr);
                } catch (NumberFormatException nfe) {
                }
            }
        }

        StringBuilder exceptionMessageBuf = new StringBuilder();

        long nowMs = System.currentTimeMillis();

        if (lastPacketSentTimeMs == 0) {
            lastPacketSentTimeMs = nowMs;
        }

        long timeSinceLastPacketSentMs = nowMs - lastPacketSentTimeMs;
        long timeSinceLastPacketSeconds = timeSinceLastPacketSentMs / 1000;

        long timeSinceLastPacketReceivedMs = nowMs - lastPacketReceivedTimeMs;

        int dueToTimeout = DUE_TO_TIMEOUT_FALSE;

        StringBuilder timeoutMessageBuf = null;

        if (serverTimeoutSeconds != 0) {
            if (timeSinceLastPacketSeconds > serverTimeoutSeconds) {
                dueToTimeout = DUE_TO_TIMEOUT_TRUE;

                timeoutMessageBuf = new StringBuilder();
                timeoutMessageBuf.append(Messages.getString("CommunicationsException.2"));
                timeoutMessageBuf.append(Messages.getString("CommunicationsException.3"));
            }

        } else if (timeSinceLastPacketSeconds > DEFAULT_WAIT_TIMEOUT_SECONDS) {
            dueToTimeout = DUE_TO_TIMEOUT_MAYBE;

            timeoutMessageBuf = new StringBuilder();
            timeoutMessageBuf.append(Messages.getString("CommunicationsException.5"));
            timeoutMessageBuf.append(Messages.getString("CommunicationsException.6"));
            timeoutMessageBuf.append(Messages.getString("CommunicationsException.7"));
            timeoutMessageBuf.append(Messages.getString("CommunicationsException.8"));
        }

        if (dueToTimeout == DUE_TO_TIMEOUT_TRUE || dueToTimeout == DUE_TO_TIMEOUT_MAYBE) {
            exceptionMessageBuf.append(lastPacketReceivedTimeMs != 0
                    ? Messages.getString("CommunicationsException.ServerPacketTimingInfo",
                            new Object[] {timeSinceLastPacketReceivedMs, timeSinceLastPacketSentMs})
                    : Messages.getString("CommunicationsException.ServerPacketTimingInfoNoRecv", new Object[] {timeSinceLastPacketSentMs}));

            exceptionMessageBuf.append(timeoutMessageBuf);
            exceptionMessageBuf.append(Messages.getString("CommunicationsException.11"));
            exceptionMessageBuf.append(Messages.getString("CommunicationsException.12"));
            exceptionMessageBuf.append(Messages.getString("CommunicationsException.13"));

        } else {
            //
            // Attempt to determine the reason for the underlying exception (we can only make a best-guess here)
            //
            if (underlyingException instanceof BindException) {
                String localSocketAddress = propertySet.getStringProperty(PropertyKey.localSocketAddress).getValue();
                boolean interfaceNotAvaliable;
                try {
                    interfaceNotAvaliable = localSocketAddress != null && NetworkInterface.getByName(localSocketAddress) == null;
                } catch (SocketException e1) {
                    interfaceNotAvaliable = false;
                }
                exceptionMessageBuf.append(interfaceNotAvaliable ? Messages.getString("CommunicationsException.LocalSocketAddressNotAvailable")
                        : Messages.getString("CommunicationsException.TooManyClientConnections"));
            }
        }

        if (exceptionMessageBuf.length() == 0) {
            // We haven't figured out a good reason, so copy it.
            exceptionMessageBuf.append(Messages.getString("CommunicationsException.20"));
            exceptionMessageBuf.append("\n\n");
            exceptionMessageBuf.append(lastPacketReceivedTimeMs != 0
                    ? Messages.getString("CommunicationsException.ServerPacketTimingInfo",
                            new Object[] {timeSinceLastPacketReceivedMs, timeSinceLastPacketSentMs})
                    : Messages.getString("CommunicationsException.ServerPacketTimingInfoNoRecv", new Object[] { Long.valueOf(timeSinceLastPacketSentMs) }));
        }

        return exceptionMessageBuf.toString();
    }

}
