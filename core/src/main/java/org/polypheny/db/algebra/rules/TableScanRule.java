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
 *
 * This file incorporates code covered by the following terms:
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
 */

package org.polypheny.db.algebra.rules;


import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.logical.LogicalTableScan;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.tools.AlgBuilderFactory;


/**
 * Planner rule that converts a {@link org.polypheny.db.algebra.logical.LogicalTableScan} to the result of calling {@link AlgOptTable#toAlg}.
 */
public class TableScanRule extends AlgOptRule {

    public static final TableScanRule INSTANCE = new TableScanRule( AlgFactories.LOGICAL_BUILDER );


    /**
     * Creates a TableScanRule.
     *
     * @param algBuilderFactory Builder for relational expressions
     */
    public TableScanRule( AlgBuilderFactory algBuilderFactory ) {
        super( operand( LogicalTableScan.class, any() ), algBuilderFactory, null );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        final LogicalTableScan oldRel = call.alg( 0 );
        AlgNode newRel = oldRel.getTable().toAlg( oldRel::getCluster );
        call.transformTo( newRel );
    }

}
