/*
 * Copyright (c) 2022, 2024, Oracle and/or its affiliates.
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
import com.mysql.cj.Messages;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.conf.RuntimeProperty;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.Message;
import com.mysql.cj.protocol.ServerSession;
import com.mysql.cj.protocol.ValueEncoder;
import com.mysql.cj.protocol.a.NativeConstants.StringLengthDataType;
import com.mysql.cj.result.Field;
import com.mysql.cj.util.StringUtils;
import com.mysql.cj.util.TimeUtil;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.LocalTime;

public abstract class AbstractValueEncoder implements ValueEncoder {

    protected PropertySet propertySet;
    protected ServerSession serverSession;
    protected RuntimeProperty<String> charEncoding = null;

    @Override
    public void init(PropertySet pset, ServerSession serverSess) {
        this.propertySet = pset;
        this.serverSession = serverSess;
        this.charEncoding = pset.getStringProperty(PropertyKey.characterEncoding);
    }

    @Override
    public byte[] getBytes(BindValue binding) throws CJException {
        return StringUtils.getBytes(getString(binding), this.charEncoding.getValue());
    }

    @Override
    public void encodeAsText(Message msg, BindValue binding) throws CJException {
        NativePacketPayload intoPacket = (NativePacketPayload) msg;
        intoPacket.writeBytes(StringLengthDataType.STRING_FIXED, getBytes(binding));
    }

    protected BigDecimal getScaled(BigDecimal x, long scaleOrLength) throws WrongArgumentException {
        BigDecimal scaledBigDecimal;
        if (scaleOrLength < 0) {
            return x.setScale(x.scale());
        }
        try {
            scaledBigDecimal = x.setScale((int) scaleOrLength);
        } catch (ArithmeticException ex) {
            try {
                scaledBigDecimal = x.setScale((int) scaleOrLength, BigDecimal.ROUND_HALF_UP);
            } catch (ArithmeticException arEx) {
                throw ExceptionFactory.createException(WrongArgumentException.class,
                        Messages.getString("PreparedStatement.65", new Object[] { scaleOrLength, x.toPlainString() }));
            }
        }
        return scaledBigDecimal;
    }

    protected LocalTime adjustLocalTime(LocalTime x, Field f) throws WrongArgumentException {
        return TimeUtil.adjustNanosPrecision(x, f == null ? 6 : f.getDecimals());
    }

    protected LocalDateTime adjustLocalDateTime(LocalDateTime x, Field f) throws WrongArgumentException {
        return TimeUtil.adjustNanosPrecision(x, f == null ? 6 : f.getDecimals());
    }

    protected Duration adjustDuration(Duration x, Field f) throws WrongArgumentException {
        return TimeUtil.adjustNanosPrecision(x, f == null ? 6 : f.getDecimals());
    }

    protected Timestamp adjustTimestamp(Timestamp x, Field f) throws WrongArgumentException {
        return TimeUtil.adjustNanosPrecision(x, f == null ? 6 : f.getDecimals());
    }

    @Override
    public long getTextLength(BindValue binding) throws CJException {
        if (binding.isNull()) {
            return 4 /* for NULL literal in SQL */;
        }
        /* for safety in escaping */
        return binding.getByteValue().length;
    }

}
