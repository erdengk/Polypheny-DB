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

package org.polypheny.db.monitoring.persistence;

import java.sql.Timestamp;
import java.util.List;
import lombok.NonNull;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.monitoring.events.MonitoringDataPoint;
import org.polypheny.db.monitoring.events.MonitoringDataPoint.DataPointType;
import org.polypheny.db.monitoring.events.metrics.DmlDataPoint;
import org.polypheny.db.monitoring.statistic.StatisticsManager;

public class StatisticRepository implements MonitoringRepository {


    @Override
    public void initialize() {

    }


    @Override
    public void persistDataPoint( @NonNull MonitoringDataPoint dataPoint ) {
        StatisticsManager<?> statisticsManager = StatisticsManager.getInstance();
        if ( dataPoint.getPointType() == DataPointType.DML ) {
            //((DmlDataPoint) dataPoint).getChangedTables().forEach( statisticsManager::addTablesToUpdate );

            DmlDataPoint dmlDataPoint = ((DmlDataPoint) dataPoint);

            if ( dmlDataPoint.isCommitted() ) {
                String name = Catalog.getInstance().getTable( dmlDataPoint.getTableId() ).name;

                if ( dmlDataPoint.getMonitoringType().equals( "INSERT" ) ) {
                    int added = dmlDataPoint.getRowsChanged();
                    statisticsManager.updateRowCountPerTable( name, added, true );
                } else if ( dmlDataPoint.getMonitoringType().equals( "DELETE" ) ) {
                    int deleted = dmlDataPoint.getRowsChanged();
                    statisticsManager.updateRowCountPerTable( name, deleted, false );
                }

                if ( dmlDataPoint.isHasIndex() ) {
                    statisticsManager.setIndexSize( name, dmlDataPoint.getIndexSize() );
                }

            }

        } else if ( dataPoint.getPointType() == DataPointType.QUERY ) {
            //statisticsManager.rowCounts( ((QueryDataPoint) dataPoint).getRowCountPerTable() );
        }


    }


    @Override
    public <T extends MonitoringDataPoint> List<T> getAllDataPoints( Class<T> dataPointClass ) {
        return null;
    }


    @Override
    public <T extends MonitoringDataPoint> List<T> getDataPointsBefore( Class<T> dataPointClass, Timestamp timestamp ) {
        return null;
    }


    @Override
    public <T extends MonitoringDataPoint> List<T> getDataPointsAfter( Class<T> dataPointClass, Timestamp timestamp ) {
        return null;
    }

}
