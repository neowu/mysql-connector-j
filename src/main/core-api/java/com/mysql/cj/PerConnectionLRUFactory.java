/*
 * Copyright (c) 2012, 2024, Oracle and/or its affiliates.
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

import com.mysql.cj.util.LRUCache;

import java.util.Set;

public class PerConnectionLRUFactory implements CacheAdapterFactory<String, QueryInfo> {

    @Override
    public CacheAdapter<String, QueryInfo> getInstance(Object syncMutex, String url, int cacheMaxSize, int maxKeySize) {
        return new PerConnectionLRU(cacheMaxSize, maxKeySize);
    }

    // this class is not thread safe anymore
    class PerConnectionLRU implements CacheAdapter<String, QueryInfo> {
        private final int cacheSqlLimit;
        private final LRUCache<String, QueryInfo> cache;

        protected PerConnectionLRU(int cacheMaxSize, int maxKeySize) {
            final int cacheSize = cacheMaxSize;
            this.cacheSqlLimit = maxKeySize;
            this.cache = new LRUCache<>(cacheSize);
        }

        @Override
        public QueryInfo get(String key) {
            if (key == null || key.length() > this.cacheSqlLimit) {
                return null;
            }


            return this.cache.get(key);

        }

        @Override
        public void put(String key, QueryInfo value) {
            if (key == null || key.length() > this.cacheSqlLimit) {
                return;
            }


            this.cache.put(key, value);

        }

        @Override
        public void invalidate(String key) {

            this.cache.remove(key);

        }

        @Override
        public void invalidateAll(Set<String> keys) {

            for (String key : keys) {
                    this.cache.remove(key);
                }

        }

        @Override
        public void invalidateAll() {

            this.cache.clear();

        }

    }

}
