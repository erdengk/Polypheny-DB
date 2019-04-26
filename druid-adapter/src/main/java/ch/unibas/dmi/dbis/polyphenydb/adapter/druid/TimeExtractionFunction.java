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

package ch.unibas.dmi.dbis.polyphenydb.adapter.druid;


import ch.unibas.dmi.dbis.polyphenydb.rex.RexCall;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexLiteral;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeName;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.avatica.util.TimeUnitRange;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexCall;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexLiteral;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeName;

import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.util.Locale;
import java.util.TimeZone;
import javax.annotation.Nullable;

import static ch.unibas.dmi.dbis.polyphenydb.adapter.druid.DruidQuery.writeFieldIf;


/**
 * Implementation of Druid time format extraction function.
 *
 * These functions return the dimension value formatted according to the given format string, time zone, and locale.
 *
 * For __time dimension values, this formats the time value bucketed by the aggregation granularity.
 */
public class TimeExtractionFunction implements ExtractionFunction {

    private static final ImmutableSet<TimeUnitRange> VALID_TIME_EXTRACT = Sets.immutableEnumSet(
            TimeUnitRange.YEAR,
            TimeUnitRange.MONTH,
            TimeUnitRange.DAY,
            TimeUnitRange.WEEK,
            TimeUnitRange.HOUR,
            TimeUnitRange.MINUTE,
            TimeUnitRange.SECOND );

    private static final ImmutableSet<TimeUnitRange> VALID_TIME_FLOOR = Sets.immutableEnumSet(
            TimeUnitRange.YEAR,
            TimeUnitRange.QUARTER,
            TimeUnitRange.MONTH,
            TimeUnitRange.DAY,
            TimeUnitRange.WEEK,
            TimeUnitRange.HOUR,
            TimeUnitRange.MINUTE,
            TimeUnitRange.SECOND );

    public static final String ISO_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

    private final String format;
    private final Granularity granularity;
    private final String timeZone;
    private final String local;


    public TimeExtractionFunction( String format, Granularity granularity, String timeZone,
            String local ) {
        this.format = format;
        this.granularity = granularity;
        this.timeZone = timeZone;
        this.local = local;
    }


    @Override
    public void write( JsonGenerator generator ) throws IOException {
        generator.writeStartObject();
        generator.writeStringField( "type", "timeFormat" );
        DruidQuery.writeFieldIf( generator, "format", format );
        DruidQuery.writeFieldIf( generator, "granularity", granularity );
        DruidQuery.writeFieldIf( generator, "timeZone", timeZone );
        DruidQuery.writeFieldIf( generator, "locale", local );
        generator.writeEndObject();
    }


    public String getFormat() {
        return format;
    }


    public Granularity getGranularity() {
        return granularity;
    }


    /**
     * Creates the default time format extraction function.
     *
     * @return the time extraction function
     */
    public static TimeExtractionFunction createDefault( String timeZone ) {
        return new TimeExtractionFunction( ISO_TIME_FORMAT, null, timeZone, null );
    }


    /**
     * Creates the time format extraction function for the given granularity.
     *
     * @param granularity granularity to apply to the column
     * @return the time extraction function corresponding to the granularity input unit {@link TimeExtractionFunction#VALID_TIME_EXTRACT} for supported granularity
     */
    public static TimeExtractionFunction createExtractFromGranularity( Granularity granularity, String timeZone ) {
        final String local = Locale.US.toLanguageTag();
        switch ( granularity.getType() ) {
            case DAY:
                return new TimeExtractionFunction( "d", null, timeZone, local );
            case MONTH:
                return new TimeExtractionFunction( "M", null, timeZone, local );
            case YEAR:
                return new TimeExtractionFunction( "yyyy", null, timeZone, local );
            case WEEK:
                return new TimeExtractionFunction( "w", null, timeZone, local );
            case HOUR:
                return new TimeExtractionFunction( "H", null, timeZone, local );
            case MINUTE:
                return new TimeExtractionFunction( "m", null, timeZone, local );
            case SECOND:
                return new TimeExtractionFunction( "s", null, timeZone, local );
            default:
                throw new IllegalArgumentException( "Granularity [" + granularity + "] is not supported" );
        }
    }


    /**
     * Creates time format floor time extraction function using a given granularity.
     *
     * @param granularity granularity to apply to the column
     * @return the time extraction function or null if granularity is not supported
     */
    public static TimeExtractionFunction createFloorFromGranularity( Granularity granularity, String timeZone ) {
        return new TimeExtractionFunction( ISO_TIME_FORMAT, granularity, timeZone, Locale.ROOT.toLanguageTag() );
    }


    /**
     * Returns whether the RexCall contains a valid extract unit that we can serialize to Druid.
     *
     * @param rexNode Extract expression
     * @return true if the extract unit is valid
     */

    public static boolean isValidTimeExtract( RexNode rexNode ) {
        final RexCall call = (RexCall) rexNode;
        if ( call.getKind() != SqlKind.EXTRACT || call.getOperands().size() != 2 ) {
            return false;
        }
        final RexLiteral flag = (RexLiteral) call.operands.get( 0 );
        final TimeUnitRange timeUnit = (TimeUnitRange) flag.getValue();
        return timeUnit != null && VALID_TIME_EXTRACT.contains( timeUnit );
    }


    /**
     * Returns whether the RexCall contains a valid FLOOR unit that we can serialize to Druid.
     *
     * @param rexNode Extract expression
     * @return true if the extract unit is valid
     */
    public static boolean isValidTimeFloor( RexNode rexNode ) {
        if ( rexNode.getKind() != SqlKind.FLOOR ) {
            return false;
        }
        final RexCall call = (RexCall) rexNode;
        if ( call.operands.size() != 2 ) {
            return false;
        }
        final RexLiteral flag = (RexLiteral) call.operands.get( 1 );
        final TimeUnitRange timeUnit = (TimeUnitRange) flag.getValue();
        return timeUnit != null && VALID_TIME_FLOOR.contains( timeUnit );
    }


    /**
     * @param rexNode cast RexNode
     * @param timeZone timezone
     * @return Druid Time extraction function or null when can not translate the cast.
     */
    @Nullable
    public static TimeExtractionFunction translateCastToTimeExtract( RexNode rexNode, TimeZone timeZone ) {
        assert rexNode.getKind() == SqlKind.CAST;
        final RexCall rexCall = (RexCall) rexNode;
        final String castFormat = DruidSqlCastConverter.dateTimeFormatString( rexCall.getType().getSqlTypeName() );
        final String timeZoneId = timeZone == null ? null : timeZone.getID();
        if ( castFormat == null ) {
            // unknown format
            return null;
        }
        SqlTypeName fromType = rexCall.getOperands().get( 0 ).getType().getSqlTypeName();
        SqlTypeName toType = rexCall.getType().getSqlTypeName();
        String granularityTZId;
        switch ( fromType ) {
            case DATE:
            case TIMESTAMP:
                granularityTZId = DateTimeUtils.UTC_ZONE.getID();
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                granularityTZId = timeZoneId;
                break;
            default:
                return null;
        }
        switch ( toType ) {
            case DATE:
                return new TimeExtractionFunction( castFormat, Granularities.createGranularity( TimeUnitRange.DAY, granularityTZId ), DateTimeUtils.UTC_ZONE.getID(), Locale.ENGLISH.toString() );
            case TIMESTAMP:
                // date -> timestamp: UTC
                // timestamp -> timestamp: UTC
                // timestamp with local time zone -> granularityTZId
                return new TimeExtractionFunction( castFormat, null, granularityTZId, Locale.ENGLISH.toString() );
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return new TimeExtractionFunction( castFormat, null, DateTimeUtils.UTC_ZONE.getID(), Locale.ENGLISH.toString() );
            default:
                return null;
        }
    }

}
