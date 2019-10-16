/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.util;


import static ch.unibas.dmi.dbis.polyphenydb.util.CaseInsensitiveComparator.COMPARATOR;

import com.google.common.collect.ImmutableSortedMap;
import java.util.Collections;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.calcite.linq4j.function.Experimental;


/**
 * Map whose keys are names and can be accessed with and without case sensitivity.
 *
 * @param <V> Value type
 */
public class NameMap<V> {

    private final NavigableMap<String, V> map;


    /**
     * Creates a NameSet based on an existing set.
     */
    private NameMap( NavigableMap<String, V> map ) {
        this.map = map;
        assert this.map.comparator() == COMPARATOR;
    }


    /**
     * Creates a NameMap, initially empty.
     */
    public NameMap() {
        this( new TreeMap<String, V>( COMPARATOR ) );
    }


    @Override
    public String toString() {
        return map.toString();
    }


    @Override
    public int hashCode() {
        return map.hashCode();
    }


    @Override
    public boolean equals( Object obj ) {
        return this == obj || obj instanceof NameMap && map.equals( ((NameMap) obj).map );
    }


    /**
     * Creates a NameMap that is an immutable copy of a given map.
     */
    public static <V> NameMap immutableCopyOf( Map<String, V> names ) {
        return new NameMap<>( ImmutableSortedMap.copyOf( names, COMPARATOR ) );
    }


    public void put( String name, V v ) {
        map.put( name, v );
    }


    /**
     * Returns a map containing all the entries in the map that match the given name. If case-sensitive, that map will have 0 or 1 elements; if
     * case-insensitive, it may have 0 or more.
     */
    public NavigableMap<String, V> range( String name, boolean caseSensitive ) {
        Object floorKey;
        Object ceilingKey;
        if ( caseSensitive ) {
            floorKey = name;
            ceilingKey = name;
        } else {
            floorKey = COMPARATOR.floorKey( name );
            ceilingKey = COMPARATOR.ceilingKey( name );
        }
        NavigableMap subMap = ((NavigableMap) map).subMap( floorKey, true, ceilingKey, true );
        return Collections.unmodifiableNavigableMap( (NavigableMap<String, V>) subMap );
    }


    /**
     * Returns whether this map contains a given key, with a given case-sensitivity.
     */
    public boolean containsKey( String name, boolean caseSensitive ) {
        return !range( name, caseSensitive ).isEmpty();
    }


    /**
     * Returns the underlying map.
     */
    public NavigableMap<String, V> map() {
        return map;
    }


    @Experimental
    public V remove( String key ) {
        return map.remove( key );
    }
}
