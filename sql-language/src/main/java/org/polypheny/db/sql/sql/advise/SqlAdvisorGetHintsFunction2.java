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

package org.polypheny.db.sql.sql.advise;


import com.google.common.collect.Iterables;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.enumerable.CallImplementor;
import org.polypheny.db.adapter.enumerable.NullPolicy;
import org.polypheny.db.adapter.enumerable.RexImpTable;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.schema.FunctionParameter;
import org.polypheny.db.schema.ImplementableFunction;
import org.polypheny.db.schema.TableFunction;
import org.polypheny.db.schema.impl.ReflectiveFunctionBase;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.Moniker;


/**
 * Table function that returns completion hints for a given SQL statement.
 * This function adds replacement string column to previously existed {@link SqlAdvisorGetHintsFunction}
 */
public class SqlAdvisorGetHintsFunction2 implements TableFunction, ImplementableFunction {

    private static final Expression ADVISOR =
            Expressions.convert_(
                    Expressions.call(
                            DataContext.ROOT,
                            BuiltInMethod.DATA_CONTEXT_GET.method,
                            Expressions.constant( DataContext.Variable.SQL_ADVISOR.camelName ) ),
                    SqlAdvisor.class );

    private static final Method GET_COMPLETION_HINTS = Types.lookupMethod( SqlAdvisorGetHintsFunction2.class, "getCompletionHints", SqlAdvisor.class, String.class, int.class );

    private static final CallImplementor IMPLEMENTOR =
            RexImpTable.createImplementor(
                    ( translator, call, operands ) ->
                            Expressions.call(
                                    GET_COMPLETION_HINTS,
                                    Iterables.concat( Collections.singleton( ADVISOR ), operands ) ),
                    NullPolicy.ANY, false );

    private static final List<FunctionParameter> PARAMETERS =
            ReflectiveFunctionBase.builder()
                    .add( String.class, "sql" )
                    .add( int.class, "pos" )
                    .build();


    @Override
    public CallImplementor getImplementor() {
        return IMPLEMENTOR;
    }


    @Override
    public AlgDataType getRowType( AlgDataTypeFactory typeFactory, List<Object> arguments ) {
        return typeFactory.createJavaType( SqlAdvisorHint2.class );
    }


    @Override
    public Type getElementType( List<Object> arguments ) {
        return SqlAdvisorHint2.class;
    }


    @Override
    public List<FunctionParameter> getParameters() {
        return PARAMETERS;
    }


    /**
     * Returns completion hints for a given SQL statement.
     *
     * Typically this is called from generated code (via {@link SqlAdvisorGetHintsFunction2#IMPLEMENTOR}).
     *
     * @param advisor Advisor to produce completion hints
     * @param sql SQL to complete
     * @param pos Cursor position in SQL
     * @return the table that contains completion hints for a given SQL statement
     */
    public static Enumerable<SqlAdvisorHint2> getCompletionHints( final SqlAdvisor advisor, final String sql, final int pos ) {
        final String[] replaced = { null };
        final List<Moniker> hints = advisor.getCompletionHints( sql, pos, replaced );
        final List<SqlAdvisorHint2> res = new ArrayList<>( hints.size() + 1 );
        res.add( new SqlAdvisorHint2( replaced[0], null, "MATCH", null ) );

        String word = replaced[0];
        for ( Moniker hint : hints ) {
            res.add( new SqlAdvisorHint2( hint, advisor.getReplacement( hint, word ) ) );
        }
        return Linq4j.asEnumerable( res ).asQueryable();
    }

}

