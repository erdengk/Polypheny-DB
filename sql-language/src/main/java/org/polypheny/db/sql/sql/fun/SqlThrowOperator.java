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

package org.polypheny.db.sql.sql.fun;


import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.sql.sql.SqlCall;
import org.polypheny.db.sql.sql.SqlNode;
import org.polypheny.db.sql.sql.SqlSpecialOperator;
import org.polypheny.db.sql.sql.SqlWriter;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.inference.ReturnTypes;


/**
 * An internal operator that throws an exception.
 *
 * The exception is thrown with a (localized) error message which is the only input parameter to the operator.
 *
 * The return type is defined as a <code>BOOLEAN</code> to facilitate the use of it in constructs such as the following:
 *
 * <blockquote><code>CASE<br>
 * WHEN &lt;conditionn&gt; THEN true<br>
 * ELSE throw("what's wrong with you man?")<br>
 * END</code></blockquote>
 */
public class SqlThrowOperator extends SqlSpecialOperator {


    public SqlThrowOperator() {
        super(
                "$throw",
                Kind.OTHER,
                2,
                true,
                ReturnTypes.BOOLEAN,
                null,
                OperandTypes.CHARACTER );
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        final SqlWriter.Frame frame = writer.startFunCall( getName() );
        ((SqlNode) call.operand( 0 )).unparse( writer, 0, 0 );
        writer.endFunCall( frame );
    }

}

