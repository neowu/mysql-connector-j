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
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.WrongArgumentException;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class DefaultPropertySet implements PropertySet, Serializable {

    private static final long serialVersionUID = -5156024634430650528L;

    private final Map<PropertyKey, RuntimeProperty<?>> PROPERTY_KEY_TO_RUNTIME_PROPERTY = new HashMap<>();

    public DefaultPropertySet() {
        for (PropertyDefinition<?> pdef : PropertyDefinitions.PROPERTY_KEY_TO_PROPERTY_DEFINITION.values()) {
            addProperty(pdef.createRuntimeProperty());
        }
    }

    @Override
    public void addProperty(RuntimeProperty<?> prop) {
        PropertyDefinition<?> def = prop.getPropertyDefinition();
        if (def.getPropertyKey() != null) {
            this.PROPERTY_KEY_TO_RUNTIME_PROPERTY.put(def.getPropertyKey(), prop);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> RuntimeProperty<T> getProperty(PropertyKey key) {
        return (RuntimeProperty<T>) this.PROPERTY_KEY_TO_RUNTIME_PROPERTY.get(key);
    }

    @Override
    public RuntimeProperty<Boolean> getBooleanProperty(PropertyKey key) {
        return getProperty(key);
    }

    @Override
    public RuntimeProperty<Integer> getIntegerProperty(PropertyKey key) {
        return getProperty(key);
    }

    @Override
    public RuntimeProperty<Integer> getMemorySizeProperty(PropertyKey key) {
        return getProperty(key);
    }

    @Override
    public RuntimeProperty<String> getStringProperty(PropertyKey key) {
        return getProperty(key);
    }

    @Override
    public <T extends Enum<T>> RuntimeProperty<T> getEnumProperty(PropertyKey key) {
        return getProperty(key);
    }

    @Override
    public void initializeProperties(Properties props) {
        if (props != null) {
            Properties infoCopy = (Properties) props.clone();

            // TODO do we need to remove next properties (as it was before)?
            infoCopy.remove(PropertyKey.HOST.getKeyName());
            infoCopy.remove(PropertyKey.PORT.getKeyName());
            infoCopy.remove(PropertyKey.USER.getKeyName());
            infoCopy.remove(PropertyKey.PASSWORD.getKeyName());
            infoCopy.remove(PropertyKey.DBNAME.getKeyName());

            for (PropertyKey propKey : PropertyDefinitions.PROPERTY_KEY_TO_PROPERTY_DEFINITION.keySet()) {
                RuntimeProperty<?> propToSet = getProperty(propKey);
                propToSet.initializeFrom(infoCopy);
            }

            // add user-defined properties
            for (Object key : infoCopy.keySet()) {
                String val = infoCopy.getProperty((String) key);
                PropertyDefinition<String> def = new StringPropertyDefinition((String) key, null, val, PropertyDefinitions.RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.unknown"), "8.0.10", PropertyDefinitions.CATEGORY_USER_DEFINED, Integer.MIN_VALUE);
                RuntimeProperty<String> p = new StringProperty(def);
                addProperty(p);
            }
        }
    }

    @Override
    public void reset() {
        this.PROPERTY_KEY_TO_RUNTIME_PROPERTY.values().forEach(RuntimeProperty::resetValue);
    }

}
