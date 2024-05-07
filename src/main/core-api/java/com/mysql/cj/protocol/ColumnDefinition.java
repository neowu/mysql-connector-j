/*
 * Copyright (c) 2016, 2024, Oracle and/or its affiliates.
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

import com.mysql.cj.result.Field;

import java.util.Map;

/**
 * Represents protocol specific result set metadata,
 * eg., for native protocol, Protocol::ColumnDefinition41 protocol entity.
 *
 */
public interface ColumnDefinition extends ProtocolEntity {

    Field[] getFields();

    void setFields(Field[] fields);

    /**
     * Builds a hash between column names and their indices for fast retrieval.
     * This is done lazily to support findColumn() and get*(String), as it
     * can be more expensive than just retrieving result set values by ordinal
     * index.
     */
    void buildIndexMapping();

    boolean hasBuiltIndexMapping();

    void setColumnToIndexCache(Map<String, Integer> columnToIndexCache);

    int findColumn(String columnName, int indexBase);

    /**
     * Check if fields with type BLOB, MEDIUMBLOB, LONGBLOB, TEXT, MEDIUMTEXT or LONGTEXT exist in this ColumnDefinition.
     *
     * @return true if fields with type BLOB, MEDIUMBLOB, LONGBLOB, TEXT, MEDIUMTEXT or LONGTEXT exist in this ColumnDefinition.
     */
    boolean hasLargeFields();

}
