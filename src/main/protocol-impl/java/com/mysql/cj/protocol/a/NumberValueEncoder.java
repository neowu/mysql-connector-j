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
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.util.StringUtils;

import java.math.BigDecimal;

public class NumberValueEncoder extends AbstractValueEncoder {

    @Override
    public String getString(BindValue binding) throws WrongArgumentException {
        Number x = binding.getValue() instanceof BigDecimal ? getScaled((BigDecimal) binding.getValue(), binding.getScaleOrLength())
                : (Number) binding.getValue();

        switch (binding.getMysqlType()) {
            case NULL:
                return "null";
            case BOOLEAN:
                return String.valueOf(x.longValue() != 0);
            case BIT:
            case TINYINT:
            case TINYINT_UNSIGNED:
            case SMALLINT:
            case SMALLINT_UNSIGNED:
            case MEDIUMINT:
            case MEDIUMINT_UNSIGNED:
            case INT:
            case YEAR:
                return String.valueOf(x.intValue());
            case INT_UNSIGNED:
            case BIGINT:
            case BIGINT_UNSIGNED:
                return String.valueOf(x.longValue());
            case FLOAT:
            case FLOAT_UNSIGNED:
                return StringUtils.fixDecimalExponent(Float.toString(x.floatValue()));
            case DOUBLE:
            case DOUBLE_UNSIGNED:
                return StringUtils.fixDecimalExponent(Double.toString(x.doubleValue()));
            case DECIMAL:
            case DECIMAL_UNSIGNED:
            case CHAR:
            case VARCHAR:
            case TINYTEXT:
            case TEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
            case BINARY:
            case VARBINARY:
            case TINYBLOB:
            case BLOB:
            case MEDIUMBLOB:
            case LONGBLOB:
                return x instanceof BigDecimal ? ((BigDecimal) x).toPlainString() : StringUtils.fixDecimalExponent(x.toString());
            default:
                throw ExceptionFactory.createException(WrongArgumentException.class,
                        Messages.getString("PreparedStatement.67", new Object[] { binding.getValue().getClass().getName(), binding.getMysqlType().toString() }));
        }
    }

}
