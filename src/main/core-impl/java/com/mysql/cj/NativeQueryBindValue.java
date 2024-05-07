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

package com.mysql.cj;

import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.protocol.Message;
import com.mysql.cj.protocol.Protocol;
import com.mysql.cj.protocol.ServerSession;
import com.mysql.cj.protocol.ValueEncoder;
import com.mysql.cj.result.Field;

import java.util.function.Supplier;

public class NativeQueryBindValue implements BindValue {

    /** NULL indicator */
    protected boolean isNull;

    protected MysqlType targetType = MysqlType.NULL;

    /** The value to store */
    public Object value;

    /** has this parameter been set? */
    protected boolean isSet = false;

    PropertySet pset;
    Protocol<?> protocol;
    ServerSession serverSession;

    private Field field = null;
    protected ValueEncoder valueEncoder = null;
    protected long scaleOrLength = -1;

    public NativeQueryBindValue(Session sess) {
        this.pset = sess.getPropertySet();
        this.protocol = ((NativeSession) sess).getProtocol();
        this.serverSession = sess.getServerSession();
    }

    @Override
    public NativeQueryBindValue copy() {
        return new NativeQueryBindValue(this);
    }

    protected NativeQueryBindValue(NativeQueryBindValue copyMe) {
        this.isNull = copyMe.isNull;
        this.targetType = copyMe.targetType;
        this.value = copyMe.value;
        this.isSet = copyMe.isSet;
        this.pset = copyMe.pset;
        this.protocol = copyMe.protocol;
        this.serverSession = copyMe.serverSession;
        this.field = copyMe.field;
        this.valueEncoder = copyMe.valueEncoder;
        this.scaleOrLength = copyMe.scaleOrLength;
    }

    @Override
    public void setBinding(Object obj, MysqlType type) {
        this.value = obj;
        this.targetType = type;

        this.isNull = this.targetType == MysqlType.NULL;
        this.isSet = true;

        Supplier<ValueEncoder> vc = this.protocol.getValueEncoderSupplier(this.isNull ? null : this.value);
        if (vc != null) {
            this.valueEncoder = vc.get();
            this.valueEncoder.init(this.pset, this.serverSession);
        } else {
            throw new Error(Messages.getString("PreparedStatement.67", new Object[] { obj.getClass().getName(), type.name() }));
        }
    }

    @Override
    public byte[] getByteValue() throws CJException {
        if (this.valueEncoder != null) {
            return this.valueEncoder.getBytes(this);
        }
        return null;
    }

    @Override
    public void reset() {
        this.isNull = false;
        this.targetType = MysqlType.NULL;
        this.value = null;
        this.isSet = false;
        this.field = null;
        this.valueEncoder = null;
        this.scaleOrLength = -1;
    }

    @Override
    public boolean isNull() {
        return this.isNull;
    }

    @Override
    public void setNull(boolean isNull) {
        this.isNull = isNull;
        if (isNull) {
            this.targetType = MysqlType.NULL;
        }
        this.isSet = true;
    }

    @Override
    public Object getValue() {
        return this.value;
    }

    @Override
    public Field getField() {
        return this.field;
    }

    @Override
    public void setField(Field field) {
        this.field = field;
    }

    @Override
    public MysqlType getMysqlType() {
        return this.targetType;
    }

    @Override
    public boolean isSet() {
        return this.isSet;
    }

    @Override
    public long getTextLength() throws CJException {
        return this.valueEncoder == null ? -1 : this.valueEncoder.getTextLength(this);
    }

    @Override
    public String getString() throws CJException {
        if (this.valueEncoder == null) {
            return "** NOT SPECIFIED **";
        }
        return this.valueEncoder.getString(this);
    }

    @Override
    public long getScaleOrLength() {
        return this.scaleOrLength;
    }

    @Override
    public void setScaleOrLength(long scaleOrLength) {
        this.scaleOrLength = scaleOrLength;
    }

    @Override
    public void writeAsText(Message intoMessage) throws CJException {
        this.valueEncoder.encodeAsText(intoMessage, this);
    }

}
