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

package ch.unibas.dmi.dbis.polyphenydb.rel.core;


import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCost;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptPlanner;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.prepare.PolyphenyDbPrepareImpl;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelInput;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelWriter;
import ch.unibas.dmi.dbis.polyphenydb.rel.SingleRel;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMdUtil;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMetadataQuery;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexChecker;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexProgram;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexShuttle;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexUtil;
import ch.unibas.dmi.dbis.polyphenydb.util.Litmus;
import com.google.common.collect.ImmutableList;
import java.util.List;


/**
 * Relational expression that iterates over its input and returns elements for which <code>condition</code> evaluates to <code>true</code>.
 *
 * If the condition allows nulls, then a null value is treated the same as false.
 *
 * @see ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalFilter
 */
public abstract class Filter extends SingleRel {

    protected final RexNode condition;


    /**
     * Creates a filter.
     *
     * @param cluster Cluster that this relational expression belongs to
     * @param traits the traits of this rel
     * @param child input relational expression
     * @param condition boolean expression which determines whether a row is allowed to pass
     */
    protected Filter( RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition ) {
        super( cluster, traits, child );
        assert condition != null;
        assert RexUtil.isFlat( condition ) : condition;
        this.condition = condition;
        // Too expensive for everyday use:
        assert !PolyphenyDbPrepareImpl.DEBUG || isValid( Litmus.THROW, null );
    }


    /**
     * Creates a Filter by parsing serialized output.
     */
    protected Filter( RelInput input ) {
        this( input.getCluster(), input.getTraitSet(), input.getInput(), input.getExpression( "condition" ) );
    }


    @Override
    public final RelNode copy( RelTraitSet traitSet, List<RelNode> inputs ) {
        return copy( traitSet, sole( inputs ), getCondition() );
    }


    public abstract Filter copy( RelTraitSet traitSet, RelNode input, RexNode condition );


    @Override
    public List<RexNode> getChildExps() {
        return ImmutableList.of( condition );
    }


    @Override
    public RelNode accept( RexShuttle shuttle ) {
        RexNode condition = shuttle.apply( this.condition );
        if ( this.condition == condition ) {
            return this;
        }
        return copy( traitSet, getInput(), condition );
    }


    public RexNode getCondition() {
        return condition;
    }


    @Override
    public boolean isValid( Litmus litmus, Context context ) {
        if ( RexUtil.isNullabilityCast( getCluster().getTypeFactory(), condition ) ) {
            return litmus.fail( "Cast for just nullability not allowed" );
        }
        final RexChecker checker = new RexChecker( getInput().getRowType(), context, litmus );
        condition.accept( checker );
        if ( checker.getFailureCount() > 0 ) {
            return litmus.fail( null );
        }
        return litmus.succeed();
    }


    @Override
    public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
        double dRows = mq.getRowCount( this );
        double dCpu = mq.getRowCount( getInput() );
        double dIo = 0;
        return planner.getCostFactory().makeCost( dRows, dCpu, dIo );
    }


    @Override
    public double estimateRowCount( RelMetadataQuery mq ) {
        return RelMdUtil.estimateFilteredRows( getInput(), condition, mq );
    }


    @Deprecated // to be removed before 2.0
    public static double estimateFilteredRows( RelNode child, RexProgram program ) {
        final RelMetadataQuery mq = RelMetadataQuery.instance();
        return RelMdUtil.estimateFilteredRows( child, program, mq );
    }


    @Deprecated // to be removed before 2.0
    public static double estimateFilteredRows( RelNode child, RexNode condition ) {
        final RelMetadataQuery mq = RelMetadataQuery.instance();
        return RelMdUtil.estimateFilteredRows( child, condition, mq );
    }


    @Override
    public RelWriter explainTerms( RelWriter pw ) {
        return super.explainTerms( pw ).item( "condition", condition );
    }
}
