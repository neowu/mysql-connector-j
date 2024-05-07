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

package com.mysql.cj.conf;

import com.mysql.cj.Messages;

import javax.naming.RefAddr;
import javax.naming.Reference;
import java.util.Properties;

public abstract class AbstractRuntimeProperty<T> implements RuntimeProperty<T> {
    private final PropertyDefinition<T> propertyDefinition;

    protected T value;

    protected T initialValue;

    protected boolean wasExplicitlySet = false;

    protected AbstractRuntimeProperty(PropertyDefinition<T> propertyDefinition) {
        this.propertyDefinition = propertyDefinition;
        this.value = propertyDefinition.getDefaultValue();
        this.initialValue = propertyDefinition.getDefaultValue();
    }

    @Override
    public PropertyDefinition<T> getPropertyDefinition() {
        return this.propertyDefinition;
    }

    @Override
    public void initializeFrom(Properties extractFrom) {
        String name = getPropertyDefinition().getName();
        String alias = getPropertyDefinition().getCcAlias();
        if (extractFrom.containsKey(name)) {
            String extractedValue = (String) extractFrom.remove(name);
            if (extractedValue != null) {
                setValueInternal(extractedValue);
                this.initialValue = this.value;
            }
        } else if (alias != null && extractFrom.containsKey(alias)) {
            String extractedValue = (String) extractFrom.remove(alias);
            if (extractedValue != null) {
                setValueInternal(extractedValue);
                this.initialValue = this.value;
            }
        }
    }

    @Override
    public void initializeFrom(Reference ref) {
        RefAddr refAddr = ref.get(getPropertyDefinition().getName());
        if (refAddr != null) {
            String refContentAsString = (String) refAddr.getContent();
            if (refContentAsString != null) {
                setValueInternal(refContentAsString);
                this.initialValue = this.value;
            }
        }
    }

    @Override
    public void resetValue() {
        this.value = this.initialValue;
    }

    @Override
    public boolean isExplicitlySet() {
        return this.wasExplicitlySet;
    }

    @Override
    public T getValue() {
        return this.value;
    }

    @Override
    public T getInitialValue() {
        return this.initialValue;
    }

    @Override
    public String getStringValue() {
        return this.value == null ? null : this.value.toString();
    }

    /**
     * Set the value of a property from a string value.
     *
     * @param value
     *            value
     */
    public void setValueInternal(String value) {
        setValueInternal(getPropertyDefinition().parseObject(value), value);
    }

    /**
     * Internal method for setting property value; ignoring the RUNTIME_NOT_MODIFIABLE flag.
     *
     * @param value
     *            value
     * @param valueAsString
     *            value represented by String
     */
    public void setValueInternal(T value, String valueAsString) {
        if (getPropertyDefinition().isRangeBased()) {
            checkRange(value, valueAsString);
        }
        this.value = value;
        this.wasExplicitlySet = true;
    }

    /**
     * For range-based property, checks that value fit into range given by PropertyDefinition.
     *
     * @param val
     *            value
     * @param valueAsString
     *            value represented by String
     */
    protected void checkRange(T val, String valueAsString) {
        // no-op for not range-based properties
    }

    @Override
    public void setValue(T value) {
        if (getPropertyDefinition().isRuntimeModifiable()) {
            setValueInternal(value, null);
        } else {
            throw new Error(
                    Messages.getString("ConnectionProperties.dynamicChangeIsNotAllowed", new Object[] { "'" + getPropertyDefinition().getName() + "'" }));
        }
    }

}
