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


import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.calcite.linq4j.Ord;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.Aggregate;
import org.polypheny.db.algebra.core.Aggregate.Group;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Join;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.fun.AggFunction;
import org.polypheny.db.algebra.logical.LogicalAggregate;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.Catalog.QueryLanguage;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.AlgBuilderFactory;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.ImmutableIntList;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Util;


/**
 * Planner rule that expands distinct aggregates (such as {@code COUNT(DISTINCT x)}) from a {@link org.polypheny.db.algebra.core.Aggregate}.
 *
 * How this is done depends upon the arguments to the function. If all functions have the same argument (e.g. {@code COUNT(DISTINCT x), SUM(DISTINCT x)} both have the argument
 * {@code x}) then one extra {@link org.polypheny.db.algebra.core.Aggregate} is sufficient.
 *
 * If there are multiple arguments (e.g. {@code COUNT(DISTINCT x), COUNT(DISTINCT y)}) the rule creates separate {@code Aggregate}s and combines using a {@link Join}.
 */
public final class AggregateExpandDistinctAggregatesRule extends AlgOptRule {

    /**
     * The default instance of the rule; operates only on logical expressions.
     */
    public static final AggregateExpandDistinctAggregatesRule INSTANCE = new AggregateExpandDistinctAggregatesRule( LogicalAggregate.class, true, AlgFactories.LOGICAL_BUILDER );

    /**
     * Instance of the rule that operates only on logical expressions and generates a join.
     */
    public static final AggregateExpandDistinctAggregatesRule JOIN = new AggregateExpandDistinctAggregatesRule( LogicalAggregate.class, false, AlgFactories.LOGICAL_BUILDER );

    public final boolean useGroupingSets;


    public AggregateExpandDistinctAggregatesRule( Class<? extends Aggregate> clazz, boolean useGroupingSets, AlgBuilderFactory algBuilderFactory ) {
        super( operand( clazz, any() ), algBuilderFactory, null );
        this.useGroupingSets = useGroupingSets;
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        final Aggregate aggregate = call.alg( 0 );
        if ( !aggregate.containsDistinctCall() ) {
            return;
        }

        // Find all of the agg expressions. We use a LinkedHashSet to ensure determinism.
        int nonDistinctAggCallCount = 0;  // find all aggregate calls without distinct
        int filterCount = 0;
        int unsupportedNonDistinctAggCallCount = 0;
        final Set<Pair<List<Integer>, Integer>> argLists = new LinkedHashSet<>();
        for ( AggregateCall aggCall : aggregate.getAggCallList() ) {
            if ( aggCall.filterArg >= 0 ) {
                ++filterCount;
            }
            if ( !aggCall.isDistinct() ) {
                ++nonDistinctAggCallCount;
                final Kind aggCallKind = aggCall.getAggregation().getKind();
                // We only support COUNT/SUM/MIN/MAX for the "single" count distinct optimization
                switch ( aggCallKind ) {
                    case COUNT:
                    case SUM:
                    case SUM0:
                    case MIN:
                    case MAX:
                        break;
                    default:
                        ++unsupportedNonDistinctAggCallCount;
                }
            } else {
                argLists.add( Pair.of( aggCall.getArgList(), aggCall.filterArg ) );
            }
        }

        final int distinctAggCallCount = aggregate.getAggCallList().size() - nonDistinctAggCallCount;
        Preconditions.checkState( argLists.size() > 0, "containsDistinctCall lied" );

        // If all of the agg expressions are distinct and have the same arguments then we can use a more efficient form.
        if ( nonDistinctAggCallCount == 0 && argLists.size() == 1 && aggregate.getGroupType() == Group.SIMPLE ) {
            final Pair<List<Integer>, Integer> pair = Iterables.getOnlyElement( argLists );
            final AlgBuilder algBuilder = call.builder();
            convertMonopole( algBuilder, aggregate, pair.left, pair.right );
            call.transformTo( algBuilder.build() );
            return;
        }

        if ( useGroupingSets ) {
            rewriteUsingGroupingSets( call, aggregate );
            return;
        }

        // If only one distinct aggregate and one or more non-distinct aggregates, we can generate multi-phase aggregates
        if ( distinctAggCallCount == 1 // one distinct aggregate
                && filterCount == 0 // no filter
                && unsupportedNonDistinctAggCallCount == 0 // sum/min/max/count in non-distinct aggregate
                && nonDistinctAggCallCount > 0 ) { // one or more non-distinct aggregates
            final AlgBuilder algBuilder = call.builder();
            convertSingletonDistinct( algBuilder, aggregate, argLists );
            call.transformTo( algBuilder.build() );
            return;
        }

        // Create a list of the expressions which will yield the final result. Initially, the expressions point to the input field.
        final List<AlgDataTypeField> aggFields = aggregate.getRowType().getFieldList();
        final List<RexInputRef> refs = new ArrayList<>();
        final List<String> fieldNames = aggregate.getRowType().getFieldNames();
        final ImmutableBitSet groupSet = aggregate.getGroupSet();
        final int groupAndIndicatorCount = aggregate.getGroupCount() + aggregate.getIndicatorCount();
        for ( int i : Util.range( groupAndIndicatorCount ) ) {
            refs.add( RexInputRef.of( i, aggFields ) );
        }

        // Aggregate the original relation, including any non-distinct aggregates.
        final List<AggregateCall> newAggCallList = new ArrayList<>();
        int i = -1;
        for ( AggregateCall aggCall : aggregate.getAggCallList() ) {
            ++i;
            if ( aggCall.isDistinct() ) {
                refs.add( null );
                continue;
            }
            refs.add( new RexInputRef( groupAndIndicatorCount + newAggCallList.size(), aggFields.get( groupAndIndicatorCount + i ).getType() ) );
            newAggCallList.add( aggCall );
        }

        // In the case where there are no non-distinct aggregates (regardless of whether there are group bys), there's no need to generate the extra aggregate and join.
        final AlgBuilder algBuilder = call.builder();
        algBuilder.push( aggregate.getInput() );
        int n = 0;
        if ( !newAggCallList.isEmpty() ) {
            final AlgBuilder.GroupKey groupKey = algBuilder.groupKey( groupSet, aggregate.getGroupSets() );
            algBuilder.aggregate( groupKey, newAggCallList );
            ++n;
        }

        // For each set of operands, find and rewrite all calls which have that set of operands.
        for ( Pair<List<Integer>, Integer> argList : argLists ) {
            doRewrite( algBuilder, aggregate, n++, argList.left, argList.right, refs );
        }

        algBuilder.project( refs, fieldNames );
        call.transformTo( algBuilder.build() );
    }


    /**
     * Converts an aggregate with one distinct aggregate and one or more non-distinct aggregates to multi-phase aggregates (see reference example below).
     *
     * @param algBuilder Contains the input relational expression
     * @param aggregate Original aggregate
     * @param argLists Arguments and filters to the distinct aggregate function
     */
    private AlgBuilder convertSingletonDistinct( AlgBuilder algBuilder, Aggregate aggregate, Set<Pair<List<Integer>, Integer>> argLists ) {

        // In this case, we are assuming that there is a single distinct function. So make sure that argLists is of size one.
        Preconditions.checkArgument( argLists.size() == 1 );

        // For example,
        //    SELECT deptno, COUNT(*), SUM(bonus), MIN(DISTINCT sal)
        //    FROM emp
        //    GROUP BY deptno
        //
        // becomes
        //
        //    SELECT deptno, SUM(cnt), SUM(bonus), MIN(sal)
        //    FROM (
        //          SELECT deptno, COUNT(*) as cnt, SUM(bonus), sal
        //          FROM EMP
        //          GROUP BY deptno, sal)            // Aggregate B
        //    GROUP BY deptno                        // Aggregate A
        algBuilder.push( aggregate.getInput() );

        final List<AggregateCall> originalAggCalls = aggregate.getAggCallList();
        final ImmutableBitSet originalGroupSet = aggregate.getGroupSet();

        // Add the distinct aggregate column(s) to the group-by columns, if not already a part of the group-by
        final SortedSet<Integer> bottomGroups = new TreeSet<>();
        bottomGroups.addAll( aggregate.getGroupSet().asList() );
        for ( AggregateCall aggCall : originalAggCalls ) {
            if ( aggCall.isDistinct() ) {
                bottomGroups.addAll( aggCall.getArgList() );
                break;  // since we only have single distinct call
            }
        }
        final ImmutableBitSet bottomGroupSet = ImmutableBitSet.of( bottomGroups );

        // Generate the intermediate aggregate B, the one on the bottom that converts a distinct call to group by call.
        // Bottom aggregate is the same as the original aggregate, except that the bottom aggregate has converted the DISTINCT aggregate to a group by clause.
        final List<AggregateCall> bottomAggregateCalls = new ArrayList<>();
        for ( AggregateCall aggCall : originalAggCalls ) {
            // Project the column corresponding to the distinct aggregate. Project as-is all the non-distinct aggregates
            if ( !aggCall.isDistinct() ) {
                final AggregateCall newCall = AggregateCall.create( (Operator & AggFunction) aggCall.getAggregation(), false, aggCall.isApproximate(), aggCall.getArgList(), -1, aggCall.collation, bottomGroupSet.cardinality(), algBuilder.peek(), null, aggCall.name );
                bottomAggregateCalls.add( newCall );
            }
        }
        // Generate the aggregate B (see the reference example above)
        algBuilder.push( aggregate.copy( aggregate.getTraitSet(), algBuilder.build(), false, bottomGroupSet, null, bottomAggregateCalls ) );

        // Add aggregate A (see the reference example above), the top aggregate to handle the rest of the aggregation that the bottom aggregate hasn't handled
        final List<AggregateCall> topAggregateCalls = new ArrayList<>();
        // Use the remapped arguments for the (non)distinct aggregate calls
        int nonDistinctAggCallProcessedSoFar = 0;
        for ( AggregateCall aggCall : originalAggCalls ) {
            final AggregateCall newCall;
            if ( aggCall.isDistinct() ) {
                List<Integer> newArgList = new ArrayList<>();
                for ( int arg : aggCall.getArgList() ) {
                    newArgList.add( bottomGroups.headSet( arg ).size() );
                }
                newCall = AggregateCall.create(
                        (Operator & AggFunction) aggCall.getAggregation(),
                        false,
                        aggCall.isApproximate(),
                        newArgList,
                        -1,
                        aggCall.collation,
                        originalGroupSet.cardinality(),
                        algBuilder.peek(),
                        aggCall.getType(),
                        aggCall.name );
            } else {
                // If aggregate B had a COUNT aggregate call the corresponding aggregate at aggregate A must be SUM. For other aggregates, it remains the same.
                final int arg = bottomGroups.size() + nonDistinctAggCallProcessedSoFar;
                final List<Integer> newArgs = ImmutableList.of( arg );
                if ( aggCall.getAggregation().getKind() == Kind.COUNT ) {
                    newCall = AggregateCall.create( (Operator & AggFunction) LanguageManager.getInstance().createSumEmptyIsZeroFunction( QueryLanguage.SQL ), false, aggCall.isApproximate(), newArgs, -1, aggCall.collation, originalGroupSet.cardinality(), algBuilder.peek(), aggCall.getType(), aggCall.getName() );
                } else {
                    newCall = AggregateCall.create( (Operator & AggFunction) aggCall.getAggregation(), false, aggCall.isApproximate(), newArgs, -1, aggCall.collation, originalGroupSet.cardinality(), algBuilder.peek(), aggCall.getType(), aggCall.name );
                }
                nonDistinctAggCallProcessedSoFar++;
            }

            topAggregateCalls.add( newCall );
        }

        // Populate the group-by keys with the remapped arguments for aggregate A
        // The top groupset is basically an identity (first X fields of aggregate B's output), minus the distinct aggCall's input.
        final Set<Integer> topGroupSet = new HashSet<>();
        int groupSetToAdd = 0;
        for ( int bottomGroup : bottomGroups ) {
            if ( originalGroupSet.get( bottomGroup ) ) {
                topGroupSet.add( groupSetToAdd );
            }
            groupSetToAdd++;
        }
        algBuilder.push( aggregate.copy( aggregate.getTraitSet(), algBuilder.build(), aggregate.indicator, ImmutableBitSet.of( topGroupSet ), null, topAggregateCalls ) );
        return algBuilder;
    }


    private void rewriteUsingGroupingSets( AlgOptRuleCall call, Aggregate aggregate ) {
        final Set<ImmutableBitSet> groupSetTreeSet = new TreeSet<>( ImmutableBitSet.ORDERING );
        for ( AggregateCall aggCall : aggregate.getAggCallList() ) {
            if ( !aggCall.isDistinct() ) {
                groupSetTreeSet.add( aggregate.getGroupSet() );
            } else {
                groupSetTreeSet.add(
                        ImmutableBitSet.of( aggCall.getArgList() )
                                .setIf( aggCall.filterArg, aggCall.filterArg >= 0 )
                                .union( aggregate.getGroupSet() ) );
            }
        }

        final ImmutableList<ImmutableBitSet> groupSets = ImmutableList.copyOf( groupSetTreeSet );
        final ImmutableBitSet fullGroupSet = ImmutableBitSet.union( groupSets );

        final List<AggregateCall> distinctAggCalls = new ArrayList<>();
        for ( Pair<AggregateCall, String> aggCall : aggregate.getNamedAggCalls() ) {
            if ( !aggCall.left.isDistinct() ) {
                AggregateCall newAggCall = aggCall.left.adaptTo( aggregate.getInput(), aggCall.left.getArgList(), aggCall.left.filterArg, aggregate.getGroupCount(), fullGroupSet.cardinality() );
                distinctAggCalls.add( newAggCall.rename( aggCall.right ) );
            }
        }

        final AlgBuilder algBuilder = call.builder();
        algBuilder.push( aggregate.getInput() );
        final int groupCount = fullGroupSet.cardinality();

        final Map<ImmutableBitSet, Integer> filters = new LinkedHashMap<>();
        final int z = groupCount + distinctAggCalls.size();
        distinctAggCalls.add( AggregateCall.create( (Operator & AggFunction) OperatorRegistry.getAgg( OperatorName.GROUPING ), false, false, ImmutableIntList.copyOf( fullGroupSet ), -1, AlgCollations.EMPTY, groupSets.size(), algBuilder.peek(), null, "$g" ) );
        for ( Ord<ImmutableBitSet> groupSet : Ord.zip( groupSets ) ) {
            filters.put( groupSet.e, z + groupSet.i );
        }

        algBuilder.aggregate( algBuilder.groupKey( fullGroupSet, groupSets ), distinctAggCalls );
        final AlgNode distinct = algBuilder.peek();

        // GROUPING returns an integer (0 or 1). Add a project to convert those values to BOOLEAN.
        if ( !filters.isEmpty() ) {
            final List<RexNode> nodes = new ArrayList<>( algBuilder.fields() );
            final RexNode nodeZ = nodes.remove( nodes.size() - 1 );
            for ( Map.Entry<ImmutableBitSet, Integer> entry : filters.entrySet() ) {
                final long v = groupValue( fullGroupSet, entry.getKey() );
                nodes.add( algBuilder.alias( algBuilder.equals( nodeZ, algBuilder.literal( v ) ), "$g_" + v ) );
            }
            algBuilder.project( nodes );
        }

        int x = groupCount;
        final List<AggregateCall> newCalls = new ArrayList<>();
        for ( AggregateCall aggCall : aggregate.getAggCallList() ) {
            final int newFilterArg;
            final List<Integer> newArgList;
            final AggFunction aggregation;
            if ( !aggCall.isDistinct() ) {
                aggregation = OperatorRegistry.getAgg( OperatorName.MIN );
                newArgList = ImmutableIntList.of( x++ );
                newFilterArg = filters.get( aggregate.getGroupSet() );
            } else {
                aggregation = aggCall.getAggregation();
                newArgList = remap( fullGroupSet, aggCall.getArgList() );
                newFilterArg =
                        filters.get(
                                ImmutableBitSet.of( aggCall.getArgList() )
                                        .setIf( aggCall.filterArg, aggCall.filterArg >= 0 )
                                        .union( aggregate.getGroupSet() ) );
            }
            final AggregateCall newCall = AggregateCall.create( (Operator & AggFunction) aggregation, false, aggCall.isApproximate(), newArgList, newFilterArg, aggCall.collation, aggregate.getGroupCount(), distinct, null, aggCall.name );
            newCalls.add( newCall );
        }

        algBuilder.aggregate( algBuilder.groupKey( remap( fullGroupSet, aggregate.getGroupSet() ), remap( fullGroupSet, aggregate.getGroupSets() ) ), newCalls );
        algBuilder.convert( aggregate.getRowType(), true );
        call.transformTo( algBuilder.build() );
    }


    private static long groupValue( ImmutableBitSet fullGroupSet, ImmutableBitSet groupSet ) {
        long v = 0;
        long x = 1L << (fullGroupSet.cardinality() - 1);
        assert fullGroupSet.contains( groupSet );
        for ( int i : fullGroupSet ) {
            if ( !groupSet.get( i ) ) {
                v |= x;
            }
            x >>= 1;
        }
        return v;
    }


    private static ImmutableBitSet remap( ImmutableBitSet groupSet, ImmutableBitSet bitSet ) {
        final ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
        for ( Integer bit : bitSet ) {
            builder.set( remap( groupSet, bit ) );
        }
        return builder.build();
    }


    private static ImmutableList<ImmutableBitSet> remap( ImmutableBitSet groupSet, Iterable<ImmutableBitSet> bitSets ) {
        final ImmutableList.Builder<ImmutableBitSet> builder = ImmutableList.builder();
        for ( ImmutableBitSet bitSet : bitSets ) {
            builder.add( remap( groupSet, bitSet ) );
        }
        return builder.build();
    }


    private static List<Integer> remap( ImmutableBitSet groupSet, List<Integer> argList ) {
        ImmutableIntList list = ImmutableIntList.of();
        for ( int arg : argList ) {
            list = list.append( remap( groupSet, arg ) );
        }
        return list;
    }


    private static int remap( ImmutableBitSet groupSet, int arg ) {
        return arg < 0 ? -1 : groupSet.indexOf( arg );
    }


    /**
     * Converts an aggregate relational expression that contains just one distinct aggregate function (or perhaps several over the same arguments) and no non-distinct aggregate functions.
     */
    private AlgBuilder convertMonopole( AlgBuilder algBuilder, Aggregate aggregate, List<Integer> argList, int filterArg ) {
        // For example,
        //    SELECT deptno, COUNT(DISTINCT sal), SUM(DISTINCT sal)
        //    FROM emp
        //    GROUP BY deptno
        //
        // becomes
        //
        //    SELECT deptno, COUNT(distinct_sal), SUM(distinct_sal)
        //    FROM (
        //      SELECT DISTINCT deptno, sal AS distinct_sal
        //      FROM EMP GROUP BY deptno)
        //    GROUP BY deptno

        // Project the columns of the GROUP BY plus the arguments to the agg function.
        final Map<Integer, Integer> sourceOf = new HashMap<>();
        createSelectDistinct( algBuilder, aggregate, argList, filterArg, sourceOf );

        // Create an aggregate on top, with the new aggregate list.
        final List<AggregateCall> newAggCalls = Lists.newArrayList( aggregate.getAggCallList() );
        rewriteAggCalls( newAggCalls, argList, sourceOf );
        final int cardinality = aggregate.getGroupSet().cardinality();
        algBuilder.push( aggregate.copy( aggregate.getTraitSet(), algBuilder.build(), aggregate.indicator, ImmutableBitSet.range( cardinality ), null, newAggCalls ) );
        return algBuilder;
    }


    /**
     * Converts all distinct aggregate calls to a given set of arguments.
     *
     * This method is called several times, one for each set of arguments. Each time it is called, it generates a JOIN to a new SELECT DISTINCT relational expression, and modifies the set of top-level calls.
     *
     * @param aggregate Original aggregate
     * @param n Ordinal of this in a join. {@code algBuilder} contains the input relational expression (either the original aggregate, the output from the previous call to this method. {@code n} is 0 if we're converting the first distinct aggregate in a query with no non-distinct aggregates)
     * @param argList Arguments to the distinct aggregate function
     * @param filterArg Argument that filters input to aggregate function, or -1
     * @param refs Array of expressions which will be the projected by the result of this rule. Those relating to this arg list will be modified  @return Relational expression
     */
    private void doRewrite( AlgBuilder algBuilder, Aggregate aggregate, int n, List<Integer> argList, int filterArg, List<RexInputRef> refs ) {
        final RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
        final List<AlgDataTypeField> leftFields;
        if ( n == 0 ) {
            leftFields = null;
        } else {
            leftFields = algBuilder.peek().getRowType().getFieldList();
        }

        // Aggregate(
        //     child,
        //     {COUNT(DISTINCT 1), SUM(DISTINCT 1), SUM(2)})
        //
        // becomes
        //
        // Aggregate(
        //     Join(
        //         child,
        //         Aggregate(child, < all columns > {}),
        //         INNER,
        //         <f2 = f5>))
        //
        // E.g.
        //   SELECT deptno, SUM(DISTINCT sal), COUNT(DISTINCT gender), MAX(age)
        //   FROM Emps
        //   GROUP BY deptno
        //
        // becomes
        //
        //   SELECT e.deptno, adsal.sum_sal, adgender.count_gender, e.max_age
        //   FROM (
        //     SELECT deptno, MAX(age) as max_age
        //     FROM Emps GROUP BY deptno) AS e
        //   JOIN (
        //     SELECT deptno, COUNT(gender) AS count_gender FROM (
        //       SELECT DISTINCT deptno, gender FROM Emps) AS dgender
        //     GROUP BY deptno) AS adgender
        //     ON e.deptno = adgender.deptno
        //   JOIN (
        //     SELECT deptno, SUM(sal) AS sum_sal FROM (
        //       SELECT DISTINCT deptno, sal FROM Emps) AS dsal
        //     GROUP BY deptno) AS adsal
        //   ON e.deptno = adsal.deptno
        //   GROUP BY e.deptno
        //
        // Note that if a query contains no non-distinct aggregates, then the very first join/group by is omitted.  In the example above, if MAX(age) is removed, then the sub-select of "e" is not needed, and instead the two other group by's are joined to one another.

        // Project the columns of the GROUP BY plus the arguments to the agg function.
        final Map<Integer, Integer> sourceOf = new HashMap<>();
        createSelectDistinct( algBuilder, aggregate, argList, filterArg, sourceOf );

        // Now compute the aggregate functions on top of the distinct dataset.
        // Each distinct agg becomes a non-distinct call to the corresponding field from the right; for example,
        //   "COUNT(DISTINCT e.sal)"
        // becomes
        //   "COUNT(distinct_e.sal)".
        final List<AggregateCall> aggCallList = new ArrayList<>();
        final List<AggregateCall> aggCalls = aggregate.getAggCallList();

        final int groupAndIndicatorCount = aggregate.getGroupCount() + aggregate.getIndicatorCount();
        int i = groupAndIndicatorCount - 1;
        for ( AggregateCall aggCall : aggCalls ) {
            ++i;

            // Ignore agg calls which are not distinct or have the wrong set arguments. If we're rewriting aggs whose args are {sal}, we will rewrite COUNT(DISTINCT sal) and SUM(DISTINCT sal) but ignore
            // COUNT(DISTINCT gender) or SUM(sal).
            if ( !aggCall.isDistinct() ) {
                continue;
            }
            if ( !aggCall.getArgList().equals( argList ) ) {
                continue;
            }

            // Re-map arguments.
            final int argCount = aggCall.getArgList().size();
            final List<Integer> newArgs = new ArrayList<>( argCount );
            for ( int j = 0; j < argCount; j++ ) {
                final Integer arg = aggCall.getArgList().get( j );
                newArgs.add( sourceOf.get( arg ) );
            }
            final int newFilterArg = aggCall.filterArg >= 0 ? sourceOf.get( aggCall.filterArg ) : -1;
            final AggregateCall newAggCall = AggregateCall.create( aggCall.getAggregation(), false, aggCall.isApproximate(), newArgs, newFilterArg, aggCall.collation, aggCall.getType(), aggCall.getName() );
            assert refs.get( i ) == null;
            if ( n == 0 ) {
                refs.set( i, new RexInputRef( groupAndIndicatorCount + aggCallList.size(), newAggCall.getType() ) );
            } else {
                refs.set( i, new RexInputRef( leftFields.size() + groupAndIndicatorCount + aggCallList.size(), newAggCall.getType() ) );
            }
            aggCallList.add( newAggCall );
        }

        final Map<Integer, Integer> map = new HashMap<>();
        for ( Integer key : aggregate.getGroupSet() ) {
            map.put( key, map.size() );
        }
        final ImmutableBitSet newGroupSet = aggregate.getGroupSet().permute( map );
        assert newGroupSet.equals( ImmutableBitSet.range( aggregate.getGroupSet().cardinality() ) );
        ImmutableList<ImmutableBitSet> newGroupingSets = null;
        if ( aggregate.indicator ) {
            newGroupingSets = ImmutableBitSet.ORDERING.immutableSortedCopy( ImmutableBitSet.permute( aggregate.getGroupSets(), map ) );
        }

        algBuilder.push( aggregate.copy( aggregate.getTraitSet(), algBuilder.build(), aggregate.indicator, newGroupSet, newGroupingSets, aggCallList ) );

        // If there's no left child yet, no need to create the join
        if ( n == 0 ) {
            return;
        }

        // Create the join condition. It is of the form
        //  'left.f0 = right.f0 and left.f1 = right.f1 and ...'
        // where {f0, f1, ...} are the GROUP BY fields.
        final List<AlgDataTypeField> distinctFields = algBuilder.peek().getRowType().getFieldList();
        final List<RexNode> conditions = new ArrayList<>();
        for ( i = 0; i < groupAndIndicatorCount; ++i ) {
            // null values form its own group use "is not distinct from" so that the join condition allows null values to match.
            conditions.add(
                    rexBuilder.makeCall(
                            OperatorRegistry.get( OperatorName.IS_NOT_DISTINCT_FROM ),
                            RexInputRef.of( i, leftFields ),
                            new RexInputRef( leftFields.size() + i, distinctFields.get( i ).getType() ) ) );
        }

        // Join in the new 'select distinct' relation.
        algBuilder.join( JoinAlgType.INNER, conditions );
    }


    private static void rewriteAggCalls( List<AggregateCall> newAggCalls, List<Integer> argList, Map<Integer, Integer> sourceOf ) {
        // Rewrite the agg calls. Each distinct agg becomes a non-distinct call to the corresponding field from the right; for example,
        // "COUNT(DISTINCT e.sal)" becomes   "COUNT(distinct_e.sal)".
        for ( int i = 0; i < newAggCalls.size(); i++ ) {
            final AggregateCall aggCall = newAggCalls.get( i );

            // Ignore agg calls which are not distinct or have the wrong set arguments. If we're rewriting aggregates whose args are {sal}, we will rewrite COUNT(DISTINCT sal) and SUM(DISTINCT sal) but ignore
            // COUNT(DISTINCT gender) or SUM(sal).
            if ( !aggCall.isDistinct() ) {
                continue;
            }
            if ( !aggCall.getArgList().equals( argList ) ) {
                continue;
            }

            // Re-map arguments.
            final int argCount = aggCall.getArgList().size();
            final List<Integer> newArgs = new ArrayList<>( argCount );
            for ( int j = 0; j < argCount; j++ ) {
                final Integer arg = aggCall.getArgList().get( j );
                newArgs.add( sourceOf.get( arg ) );
            }
            final AggregateCall newAggCall = AggregateCall.create( aggCall.getAggregation(), false, aggCall.isApproximate(), newArgs, -1, aggCall.collation, aggCall.getType(), aggCall.getName() );
            newAggCalls.set( i, newAggCall );
        }
    }


    /**
     * Given an {@link org.polypheny.db.algebra.core.Aggregate} and the ordinals of the arguments to a particular call to an aggregate function, creates a 'select distinct' relational expression which
     * projects the group columns and those arguments but nothing else.
     *
     * For example, given
     *
     * <blockquote>
     * <pre>select f0, count(distinct f1), count(distinct f2)
     * from t group by f0</pre>
     * </blockquote>
     *
     * and the argument list
     *
     * <blockquote>{2}</blockquote>
     *
     * returns
     *
     * <blockquote>
     * <pre>select distinct f0, f2 from t</pre>
     * </blockquote>
     *
     * The <code>sourceOf</code> map is populated with the source of each column; in this case sourceOf.get(0) = 0, and sourceOf.get(1) = 2.
     *
     * @param algBuilder Relational expression builder
     * @param aggregate Aggregate relational expression
     * @param argList Ordinals of columns to make distinct
     * @param filterArg Ordinal of column to filter on, or -1
     * @param sourceOf Out parameter, is populated with a map of where each output field came from
     * @return Aggregate relational expression which projects the required columns
     */
    private AlgBuilder createSelectDistinct( AlgBuilder algBuilder, Aggregate aggregate, List<Integer> argList, int filterArg, Map<Integer, Integer> sourceOf ) {
        algBuilder.push( aggregate.getInput() );
        final List<Pair<RexNode, String>> projects = new ArrayList<>();
        final List<AlgDataTypeField> childFields = algBuilder.peek().getRowType().getFieldList();
        for ( int i : aggregate.getGroupSet() ) {
            sourceOf.put( i, projects.size() );
            projects.add( RexInputRef.of2( i, childFields ) );
        }
        for ( Integer arg : argList ) {
            if ( filterArg >= 0 ) {
                // Implement
                //   agg(DISTINCT arg) FILTER $f
                // by generating
                //   SELECT DISTINCT ... CASE WHEN $f THEN arg ELSE NULL END AS arg
                // and then applying
                //   agg(arg)
                // as usual.
                //
                // It works except for (rare) agg functions that need to see null values.
                final RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
                final RexInputRef filterRef = RexInputRef.of( filterArg, childFields );
                final Pair<RexNode, String> argRef = RexInputRef.of2( arg, childFields );
                RexNode condition =
                        rexBuilder.makeCall(
                                OperatorRegistry.get( OperatorName.CASE ),
                                filterRef,
                                argRef.left,
                                rexBuilder.ensureType(
                                        argRef.left.getType(),
                                        rexBuilder.makeCast( argRef.left.getType(), rexBuilder.constantNull() ),
                                        true ) );
                sourceOf.put( arg, projects.size() );
                projects.add( Pair.of( condition, "i$" + argRef.right ) );
                continue;
            }
            if ( sourceOf.get( arg ) != null ) {
                continue;
            }
            sourceOf.put( arg, projects.size() );
            projects.add( RexInputRef.of2( arg, childFields ) );
        }
        algBuilder.project( Pair.left( projects ), Pair.right( projects ) );

        // Get the distinct values of the GROUP BY fields and the arguments to the agg functions.
        algBuilder.push( aggregate.copy( aggregate.getTraitSet(), algBuilder.build(), false, ImmutableBitSet.range( projects.size() ), null, ImmutableList.of() ) );
        return algBuilder;
    }

}

