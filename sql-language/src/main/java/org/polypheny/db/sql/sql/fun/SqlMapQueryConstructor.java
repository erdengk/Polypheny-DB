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


/**
 * Definition of the MAP query constructor, <code>MAP (&lt;query&gt;)</code>.
 *
 * Like the MAP type, not standard SQL.
 */
public class SqlMapQueryConstructor extends SqlMultisetQueryConstructor {

    public SqlMapQueryConstructor() {
        super( "MAP", Kind.MAP_QUERY_CONSTRUCTOR );
    }

}

