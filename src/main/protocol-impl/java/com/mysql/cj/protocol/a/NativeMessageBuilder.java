/*
 * Copyright (c) 2017, 2024, Oracle and/or its affiliates.
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

import com.mysql.cj.BindValue;
import com.mysql.cj.MessageBuilder;
import com.mysql.cj.PreparedQuery;
import com.mysql.cj.Query;
import com.mysql.cj.QueryBindings;
import com.mysql.cj.Session;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.a.NativeConstants.IntegerDataType;
import com.mysql.cj.protocol.a.NativeConstants.StringLengthDataType;
import com.mysql.cj.util.StringUtils;

public class NativeMessageBuilder implements MessageBuilder<NativePacketPayload> {

    private boolean supportsQueryAttributes = true;

    public NativeMessageBuilder(boolean supportsQueryAttributes) {
        this.supportsQueryAttributes = supportsQueryAttributes;
    }

    public NativePacketPayload buildComQuery(NativePacketPayload sharedPacket, byte[] query) {
        NativePacketPayload packet = sharedPacket != null ? sharedPacket : new NativePacketPayload(query.length + 1);
        packet.writeInteger(IntegerDataType.INT1, NativeConstants.COM_QUERY);

        if (this.supportsQueryAttributes) {
            // CLIENT_QUERY_ATTRIBUTES capability has been negotiated but, since this method is used solely to run queries internally and it is not bound to any
            // Statement object, no query attributes are ever set.
            packet.writeInteger(IntegerDataType.INT_LENENC, 0);
            packet.writeInteger(IntegerDataType.INT_LENENC, 1); // parameter_set_count (always 1)
        }

        packet.writeBytes(StringLengthDataType.STRING_FIXED, query);
        return packet;
    }

    public NativePacketPayload buildComQuery(NativePacketPayload sharedPacket, String query) {
        return buildComQuery(sharedPacket, StringUtils.getBytes(query));
    }

    @Override
    public NativePacketPayload buildComQuery(NativePacketPayload sharedPacket, Session sess, String query, Query callingQuery, String characterEncoding) throws WrongArgumentException {
        final NativePacketPayload sendPacket;
        if (sharedPacket != null) {
            sendPacket = sharedPacket;
        } else {
            // Compute packet length. It's not possible to know exactly how many bytes will be obtained from the query, but UTF-8 max encoding length is 4, so
            // pad it (4 * query) + space for headers
            int packLength = 1 /* COM_QUERY */ + query.length() * 4 + 2;

            if (this.supportsQueryAttributes) {
                packLength += 1 /* parameter_count */ + 1 /* parameter_set_count */;
            }

            sendPacket = new NativePacketPayload(packLength);
        }

        sendPacket.setPosition(0);
        sendPacket.writeInteger(IntegerDataType.INT1, NativeConstants.COM_QUERY);

        if (this.supportsQueryAttributes) {
            sendPacket.writeInteger(IntegerDataType.INT_LENENC, 0);
            sendPacket.writeInteger(IntegerDataType.INT_LENENC, 1); // parameter_set_count (always 1)
        }
        sendPacket.setTag("QUERY");

        sendPacket.writeBytes(StringLengthDataType.STRING_FIXED, StringUtils.getBytes(query, characterEncoding));

        return sendPacket;
    }

    @Override
    public NativePacketPayload buildComQuery(NativePacketPayload sharedPacket, Session sess, PreparedQuery preparedQuery, QueryBindings bindings, String characterEncoding) throws CJException {
        NativePacketPayload sendPacket = sharedPacket != null ? sharedPacket : new NativePacketPayload(9);

        BindValue[] bindValues = bindings.getBindValues();

        sendPacket.writeInteger(IntegerDataType.INT1, NativeConstants.COM_QUERY);

        if (this.supportsQueryAttributes) {
            sendPacket.writeInteger(IntegerDataType.INT_LENENC, 0);
            sendPacket.writeInteger(IntegerDataType.INT_LENENC, 1); // parameter_set_count (always 1)
        }

        sendPacket.setTag("QUERY");

        byte[][] staticSqlStrings = preparedQuery.getQueryInfo().getStaticSqlParts();
        for (int i = 0; i < bindValues.length; i++) {
            bindings.checkParameterSet(i);
            sendPacket.writeBytes(StringLengthDataType.STRING_FIXED, staticSqlStrings[i]);
            bindValues[i].writeAsText(sendPacket);
        }

        sendPacket.writeBytes(StringLengthDataType.STRING_FIXED, staticSqlStrings[bindValues.length]);

        return sendPacket;
    }

    public NativePacketPayload buildComInitDb(NativePacketPayload sharedPacket, byte[] dbName) {
        NativePacketPayload packet = sharedPacket != null ? sharedPacket : new NativePacketPayload(dbName.length + 1);
        packet.writeInteger(IntegerDataType.INT1, NativeConstants.COM_INIT_DB);
        packet.writeBytes(StringLengthDataType.STRING_FIXED, dbName);
        return packet;
    }

    public NativePacketPayload buildComInitDb(NativePacketPayload sharedPacket, String dbName) {
        return buildComInitDb(sharedPacket, StringUtils.getBytes(dbName));
    }

    public NativePacketPayload buildComSetOption(NativePacketPayload sharedPacket, int val) {
        NativePacketPayload packet = sharedPacket != null ? sharedPacket : new NativePacketPayload(3);
        packet.writeInteger(IntegerDataType.INT1, NativeConstants.COM_SET_OPTION);
        packet.writeInteger(IntegerDataType.INT2, val);
        return packet;
    }

    public NativePacketPayload buildComPing(NativePacketPayload sharedPacket) {
        NativePacketPayload packet = sharedPacket != null ? sharedPacket : new NativePacketPayload(1);
        packet.writeInteger(IntegerDataType.INT1, NativeConstants.COM_PING);
        return packet;
    }

    public NativePacketPayload buildComQuit(NativePacketPayload sharedPacket) {
        NativePacketPayload packet = sharedPacket != null ? sharedPacket : new NativePacketPayload(1);
        packet.writeInteger(IntegerDataType.INT1, NativeConstants.COM_QUIT);
        return packet;
    }

    public NativePacketPayload buildComResetConnection(NativePacketPayload sharedPacket) {
        NativePacketPayload packet = sharedPacket != null ? sharedPacket : new NativePacketPayload(1);
        packet.writeInteger(IntegerDataType.INT1, NativeConstants.COM_RESET_CONNECTION);
        return packet;
    }

}
