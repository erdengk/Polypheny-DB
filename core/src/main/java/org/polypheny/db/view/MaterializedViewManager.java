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

package org.polypheny.db.view;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogMaterializedView;
import org.polypheny.db.catalog.entity.MaterializedCriteria;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.RelShuttleImpl;
import org.polypheny.db.rel.core.TableModify;
import org.polypheny.db.rel.core.TableModify.Operation;
import org.polypheny.db.rel.logical.LogicalTableModify;
import org.polypheny.db.schema.LogicalTable;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.transaction.Transaction;


public abstract class MaterializedViewManager {

    public static MaterializedViewManager INSTANCE = null;
    public boolean isDroppingMaterialized = false;
    public boolean isCreatingMaterialized = false;
    public boolean isUpdatingMaterialized = false;


    public static MaterializedViewManager setAndGetInstance( MaterializedViewManager transaction ) {
        if ( INSTANCE != null ) {
            throw new RuntimeException( "Overwriting the MaterializedViewManager is not permitted." );
        }
        INSTANCE = transaction;
        return INSTANCE;
    }


    public static MaterializedViewManager getInstance() {
        if ( INSTANCE == null ) {
            throw new RuntimeException( "MaterializedViewManager was not set correctly on Polypheny-DB start-up" );
        }
        return INSTANCE;
    }


    public abstract void deleteMaterializedViewFromInfo( Long tableId );

    public abstract void addData(
            Transaction transaction,
            List<DataStore> stores,
            Map<Integer, List<CatalogColumn>> addedColumns,
            RelRoot relRoot,
            CatalogMaterializedView materializedView );

    public abstract void addTables( Transaction transaction, List<String> names );

    public abstract void updateData( Transaction transaction, Long viewId );

    public abstract void updateCommittedXid( PolyXid xid );

    public abstract void updateMaterializedTime( Long materializedId );

    public abstract void addMaterializedInfo( Long materializedId, MaterializedCriteria matViewCriteria );


    /**
     * to trek updates on tables for materialized views with update freshness
     */
    public static class TableUpdateVisitor extends RelShuttleImpl {

        @Getter
        private final List<String> names = new ArrayList<>();

        @Getter
        Map<List<String>, List<RelNode>> insertedData = new HashMap<>();

        @Getter
        Map<List<String>, List<RelNode>> deletedData = new HashMap<>();

        @Getter
        Map<List<String>, List<RelNode>> updatedData = new HashMap<>();


        @Override
        public RelNode visit( RelNode other ) {
            if ( other instanceof LogicalTableModify ) {
                if ( ((TableModify) other).getOperation() != Operation.MERGE ) {
                    if ( (other.getTable().getTable() instanceof LogicalTable) ) {
                        List<String> qualifiedName = other.getTable().getQualifiedName();
                        if ( qualifiedName.size() < 2 ) {
                            names.add( ((LogicalTable) other.getTable().getTable()).getLogicalSchemaName() );
                            names.add( ((LogicalTable) other.getTable().getTable()).getLogicalTableName() );
                        } else {
                            names.addAll( qualifiedName );
                        }
                        if ( ((TableModify) other).getOperation() == Operation.INSERT ) {
                            insertedData.put( names, other.getInputs() );
                        }
                        if ( ((TableModify) other).getOperation() == Operation.DELETE ) {
                            deletedData.put( names, other.getInputs() );
                        }
                        if ( ((TableModify) other).getOperation() == Operation.UPDATE ) {
                            updatedData.put( names, other.getInputs() );
                        }
                    }
                }
            }
            return super.visit( other );
        }

    }

}