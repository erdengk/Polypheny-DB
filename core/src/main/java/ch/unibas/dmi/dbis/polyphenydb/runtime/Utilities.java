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

package ch.unibas.dmi.dbis.polyphenydb.runtime;


import java.util.Iterator;
import java.util.List;


/**
 * Utility methods called by generated code.
 */
public class Utilities {

    // Even though this is a utility class (all methods are static), we cannot make the constructor private. Because Janino doesn't do static import, generated code is placed in sub-classes.
    protected Utilities() {
    }


    public static int hash( Object v ) {
        return v == null ? 0 : v.hashCode();
    }

    public static int hash( int h, boolean v ) {
        return h * 31 + Boolean.hashCode( v );
    }

    public static int hash( int h, byte v ) {
        return h * 31 + v;
    }


    public static int hash( int h, char v ) {
        return h * 31 + v;
    }


    public static int hash( int h, short v ) {
        return h * 31 + v;
    }


    public static int hash( int h, int v ) {
        return h * 31 + v;
    }


    public static int hash( int h, long v ) {
        return h * 31 + Long.hashCode( v );
    }


    public static int hash( int h, float v ) {
        return hash( h, Float.hashCode( v ) );
    }


    public static int hash( int h, double v ) {
        return hash( h, Double.hashCode( v ) );
    }


    public static int hash( int h, Object v ) {
        return h * 31 + (v == null ? 1 : v.hashCode());
    }


    public static int compare( boolean v0, boolean v1 ) {
        return Boolean.compare( v0, v1 );
    }


    public static int compare( byte v0, byte v1 ) {
        return Byte.compare( v0, v1 );
    }


    public static int compare( char v0, char v1 ) {
        return Character.compare( v0, v1 );
    }


    public static int compare( short v0, short v1 ) {
        return Short.compare( v0, v1 );
    }


    public static int compare( int v0, int v1 ) {
        return Integer.compare( v0, v1 );
    }


    public static int compare( long v0, long v1 ) {
        return Long.compare( v0, v1 );
    }


    public static int compare( float v0, float v1 ) {
        return Float.compare( v0, v1 );
    }


    public static int compare( double v0, double v1 ) {
        return Double.compare( v0, v1 );
    }


    public static int compare( List v0, List v1 ) {
        final Iterator iterator0 = v0.iterator();
        final Iterator iterator1 = v1.iterator();
        for ( ; ; ) {
            if ( !iterator0.hasNext() ) {
                return !iterator1.hasNext()
                        ? 0
                        : -1;
            }
            if ( !iterator1.hasNext() ) {
                return 1;
            }
            final Object o0 = iterator0.next();
            final Object o1 = iterator1.next();
            int c = compare_( o0, o1 );
            if ( c != 0 ) {
                return c;
            }
        }
    }


    private static int compare_( Object o0, Object o1 ) {
        if ( o0 instanceof Comparable ) {
            return compare( (Comparable) o0, (Comparable) o1 );
        }
        return compare( (List) o0, (List) o1 );
    }


    public static int compare( Comparable v0, Comparable v1 ) {
        //noinspection unchecked
        return v0.compareTo( v1 );
    }


    public static int compareNullsFirst( Comparable v0, Comparable v1 ) {
        //noinspection unchecked
        return v0 == v1
                ? 0
                : v0 == null
                        ? -1
                        : v1 == null
                                ? 1
                                : v0.compareTo( v1 );
    }


    public static int compareNullsLast( Comparable v0, Comparable v1 ) {
        //noinspection unchecked
        return v0 == v1
                ? 0
                : v0 == null
                        ? 1
                        : v1 == null
                                ? -1
                                : v0.compareTo( v1 );
    }


    public static int compareNullsLast( List v0, List v1 ) {
        //noinspection unchecked
        return v0 == v1
                ? 0
                : v0 == null
                        ? 1
                        : v1 == null
                                ? -1
                                : FlatLists.ComparableListImpl.compare( v0, v1 );
    }
}

