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

package org.polypheny.db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;
import org.polypheny.db.monitoring.events.StatementEvent;
import org.polypheny.db.transaction.Statement;

public class StatisticEnumerableWrapper<T> implements Iterator<T> {

    private final Iterator<T> iterator;
    private static final AtomicLong idBuilder = new AtomicLong();
    private long id;
    private String kind;
    @Getter
    private int rowsChanged;
    private Statement statement;


    public StatisticEnumerableWrapper( Iterator<T> iterator, Statement statement ) {
        this.iterator = iterator;
        this.statement = statement;
        this.kind = statement.getTransaction().getMonitoringData().getMonitoringType();
        getRowsChangedForDML( iterator, statement );

        this.id = idBuilder.getAndIncrement();
    }


    private void getRowsChangedForDML( Iterator<T> iterator, Statement statement ) {
        Object object;
        if ( kind.equals( "UPDATE" ) || kind.equals( "DELETE" ) || kind.equals( "MERGE" ) ) {
            while ( iterator.hasNext() ) {
                object = iterator.next();
                int num;
                if ( object != null && object.getClass().isArray() ) {
                    Object[] o = (Object[]) object;
                    num = ((Number) o[0]).intValue();
                } else if ( object != null ) {
                    num = ((Number) object).intValue();
                } else {
                    throw new RuntimeException( "Result is null" );
                }
                // Check if num is equal for all adapters
                if ( rowsChanged != -1 && rowsChanged != num ) {
                    //throw new QueryExecutionException( "The number of changed rows is not equal for all stores!" );
                }
                rowsChanged = num;
            }
        } else if ( kind.equals( "INSERT" ) ) {
            while ( iterator.hasNext() ) {
                iterator.next();
            }
            rowsChanged = statement.getDataContext().getParameterValues().size();
        }

        HashMap<Long, List<Object>> ordered = new HashMap<>();

        List<Map<Long, Object>> values = statement.getDataContext().getParameterValues();
        if ( values.size() > 0 ) {
            for ( long i = 0; i < statement.getDataContext().getParameterValues().get( 0 ).size(); i++ ) {
                ordered.put( i, new ArrayList<>() );
            }
        }

        for ( Map<Long, Object> longObjectMap : statement.getDataContext().getParameterValues() ) {
            longObjectMap.forEach( ( k, v ) -> {
                ordered.get( k ).add( v );
            } );
        }

        /*
        Map<Long, List<Object>> interestingVals = new HashMap<>();

        Map<Long, List<Object>> min = new HashMap<>();
        Map<Long, List<Object>> max = new HashMap<>();

        for ( Entry<Long, List<Object>> entry : ordered.entrySet() ) {
            List<Object> list =  entry.getValue().stream().sorted().collect( Collectors.toList());
            List<Object> merged = new ArrayList<>();
            merged.addAll( list.subList( 0, 4 ) );
            merged.addAll( list.subList(list.size()-5, list.size()-1)  );
            min.put( entry.getKey(), list.subList( 0, 4 ) );
            max.put( entry.getKey(), list.subList( list.size()-5, list.size()-1 ) );
        }
         */

        StatementEvent ev = statement.getTransaction().getMonitoringData();
        ev.setChangedVals( ordered );
        ev.setRowsChanged( rowsChanged ); // for statistics to count total amount of rows
        ev.setRowCount( rowsChanged ); // for workload monitoring
    }


    @Override
    public boolean hasNext() {
        boolean hasNext = iterator.hasNext();
        if ( !hasNext ) {
            System.out.println( "I am so empty pls tell someone!" + id );
        }
        return hasNext;
    }


    @Override
    public T next() {
        T ob = iterator.next();
        if ( !iterator.hasNext() ) {
            System.out.println( "I am so empty pls tell someone!" + id );
        }
        return ob;
    }

}
