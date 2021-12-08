/*
 * Copyright 2019-2021 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.monitoring.statistic;

import java.util.List;
import java.util.TreeMap;
import lombok.Getter;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;

@Getter
public class StatInfo<T extends Comparable<T>> {

    private final long tableId;

    private final long columnId;

    //private final StatInfoType statInfoType;

    private final TreeMap<Object, Integer> uniqueValue;

    private final TreeMap<T, Integer> minValue;

    private final TreeMap<T, Integer> maxValue;

    private boolean isFull;

    private final int STATCACHE = 10;

    private final PolyType polyType;


    public StatInfo( long tableId, long columnId ) {
        this.tableId = tableId;
        this.columnId = columnId;
        this.polyType = Catalog.getInstance().getColumn( columnId ).type;
        this.isFull = false;
        uniqueValue = new TreeMap<>();
        minValue = new TreeMap<>();
        maxValue = new TreeMap<>();
    }


    public void insert( Object val ) {
        if ( uniqueValue.containsKey( val ) ) {
            int times = uniqueValue.get( val );
            uniqueValue.remove( val );
            uniqueValue.put( val, times + 1 );
        } else {
            if ( uniqueValue.size() < STATCACHE ) {
                uniqueValue.put( val, 1 );
            } else {
                if ( polyType.getFamily() == PolyTypeFamily.NUMERIC ) {
                    isFull = true;
                    /*
                    minValue.putAll(  uniqueValue );
                    if(minValue.lastKey().compareTo( val ) > 0){
                        minValue.remove( minValue.lastKey() );
                        minValue.put( val, 1 );
                    }
                    if(maxValue.lastKey().compareTo( val ) < 0){
                        maxValue.remove( maxValue.lastKey() );
                        maxValue.put( val, 1 );
                    }

                     */
                } else if ( polyType.getFamily() == PolyTypeFamily.CHARACTER ) {
                    isFull = true;
                } else if ( PolyType.DATETIME_TYPES.contains( polyType ) ) {
                    isFull = true;
                }
            }

        }
    }


    public void delete( List<T> values ) {
        for ( T val : values ) {
            uniqueValue.remove( val );
        }
    }


    public void delete( T val ) {
        uniqueValue.remove( val );
    }


    public void insert( List<Object> values ) {
        for ( Object val : values ) {
            insert( val );
        }
    }

}

