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

package org.polypheny.db.monitoring.core;

import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.monitoring.persistence.MapDbRepository;
import org.polypheny.db.monitoring.persistence.StatisticRepository;
import org.polypheny.db.monitoring.ui.MonitoringServiceUi;
import org.polypheny.db.monitoring.ui.MonitoringServiceUiImpl;


@Slf4j
public class MonitoringServiceFactory {

    public static MonitoringServiceImpl createMonitoringService() {
        // Create mapDB repository
        MapDbRepository mapRepo = new MapDbRepository();

        // Create statistic repository
        StatisticRepository statisticRepo = new StatisticRepository();

        // Initialize the mapDB mapRepo and open connection
        mapRepo.initialize();

        // Create monitoring service with dependencies
        MonitoringQueue queueWriteService = new MonitoringQueueImpl( mapRepo, statisticRepo );
        MonitoringServiceUi uiService = new MonitoringServiceUiImpl( mapRepo, queueWriteService );

        // Initialize the monitoringService
        MonitoringServiceImpl monitoringService = new MonitoringServiceImpl( queueWriteService, mapRepo, uiService );

        return monitoringService;
    }

}
