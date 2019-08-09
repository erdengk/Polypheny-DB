/*
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
 *
 */

package ch.unibas.dmi.dbis.polyphenydb.sql.ddl.altertable;


import ch.unibas.dmi.dbis.polyphenydb.PolySqlType;
import ch.unibas.dmi.dbis.polyphenydb.Transaction;
import ch.unibas.dmi.dbis.polyphenydb.UnknownTypeException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogColumn;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.combined.CatalogCombinedTable;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.GenericCatalogException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownCollationException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownEncodingException;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.Context;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlDataTypeSpec;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlIdentifier;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlWriter;
import ch.unibas.dmi.dbis.polyphenydb.sql.ddl.SqlAlterTable;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserPos;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableNullableList;
import java.util.List;
import lombok.NonNull;


/**
 * Parse tree for {@code ALTER TABLE name MODIFY COLUMN} statement.
 */
public class SqlAlterTableModifyColumn extends SqlAlterTable {

    private final SqlIdentifier tableName;
    private final SqlIdentifier columnName;

    private final SqlDataTypeSpec type;
    private final Boolean nullable;
    private final SqlIdentifier beforeColumn;
    private final SqlIdentifier afterColumn;
    private final SqlNode defaultValue;
    private final Boolean dropDefault;


    public SqlAlterTableModifyColumn(
            SqlParserPos pos,
            @NonNull SqlIdentifier tableName,
            @NonNull SqlIdentifier columnName,
            SqlDataTypeSpec type,
            Boolean nullable,
            SqlIdentifier beforeColumn,
            SqlIdentifier afterColumn,
            SqlNode defaultValue,
            Boolean dropDefault ) {
        super( pos );
        this.tableName = tableName;
        this.columnName = columnName;
        this.type = type;
        this.nullable = nullable;
        this.beforeColumn = beforeColumn;
        this.afterColumn = afterColumn;
        this.defaultValue = defaultValue;
        this.dropDefault = dropDefault;
    }


    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of( tableName, columnName, type, beforeColumn, afterColumn, defaultValue );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "ALTER" );
        writer.keyword( "TABLE" );
        tableName.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "MODIFY" );
        writer.keyword( "COLUMN" );
        columnName.unparse( writer, leftPrec, rightPrec );
        if ( type != null ) {
            writer.keyword( "SET" );
            writer.keyword( "TYPE" );
            type.unparse( writer, leftPrec, rightPrec );
        } else if ( nullable != null && !nullable ) {
            writer.keyword( "SET" );
            writer.keyword( "NOT" );
            writer.keyword( "NULL" );
        } else if ( nullable != null && nullable ) {
            writer.keyword( "SET" );
            writer.keyword( "NULL" );
        } else if ( beforeColumn != null ) {
            writer.keyword( "SET" );
            writer.keyword( "POSITION" );
            writer.keyword( "BEFORE" );
            beforeColumn.unparse( writer, leftPrec, rightPrec );
        } else if ( afterColumn != null ) {
            writer.keyword( "SET" );
            writer.keyword( "POSITION" );
            writer.keyword( "AFTER" );
            afterColumn.unparse( writer, leftPrec, rightPrec );
        } else if ( defaultValue != null ) {
            writer.keyword( "SET" );
            writer.keyword( "DEFAULT" );
            defaultValue.unparse( writer, leftPrec, rightPrec );
        } else if ( dropDefault != null && dropDefault ) {
            writer.keyword( "DROP" );
            writer.keyword( "DEFAULT" );
        } else {
            throw new RuntimeException( "Unknown option" );
        }
    }


    @Override
    public void execute( Context context, Transaction transaction ) {
        CatalogCombinedTable catalogTable = getCatalogCombinedTable( context, transaction, tableName );
        CatalogColumn catalogColumn = getCatalogColumn( context, transaction, catalogTable.getTable().id, columnName );

        try {
            if ( type != null ) {
                PolySqlType polySqlType = PolySqlType.getPolySqlTypeFromSting( type.getTypeName().getSimple() );
                transaction.getCatalog().setColumnType(
                        catalogColumn.id,
                        polySqlType,
                        type.getScale() == -1 ? null : type.getScale(),
                        type.getPrecision() == -1 ? null : type.getPrecision() );
            } else if ( nullable != null ) {
                transaction.getCatalog().setNullable( catalogColumn.id, nullable );
            } else if ( beforeColumn != null || afterColumn != null ) {
                int targetPosition;
                CatalogColumn refColumn;
                if ( beforeColumn != null ) {
                    refColumn = getCatalogColumn( context, transaction, catalogTable.getTable().id, beforeColumn );
                    targetPosition = refColumn.position;
                } else {
                    refColumn = getCatalogColumn( context, transaction, catalogTable.getTable().id, afterColumn );
                    targetPosition = refColumn.position + 1;
                }
                if ( catalogColumn.id == refColumn.id ) {
                    throw new RuntimeException( "Same column!" );
                }
                List<CatalogColumn> columns = transaction.getCatalog().getColumns( catalogTable.getTable().id );
                if ( targetPosition < catalogColumn.position ) {  // Walk from last column to first column
                    for ( int i = columns.size(); i >= 1; i-- ) {
                        if ( i < catalogColumn.position && i >= targetPosition ) {
                            transaction.getCatalog().setColumnPosition( columns.get( i - 1 ).id, i + 1 );
                        } else if ( i == catalogColumn.position ) {
                            transaction.getCatalog().setColumnPosition( catalogColumn.id, columns.size() + 1 );
                        }
                        if ( i == targetPosition ) {
                            transaction.getCatalog().setColumnPosition( catalogColumn.id, targetPosition );
                        }
                    }
                } else if ( targetPosition > catalogColumn.position ) { // Walk from first column to last column
                    targetPosition--;
                    for ( int i = 1; i <= columns.size(); i++ ) {
                        if ( i > catalogColumn.position && i <= targetPosition ) {
                            transaction.getCatalog().setColumnPosition( columns.get( i - 1 ).id, i - 1 );
                        } else if ( i == catalogColumn.position ) {
                            transaction.getCatalog().setColumnPosition( catalogColumn.id, columns.size() + 1 );
                        }
                        if ( i == targetPosition ) {
                            transaction.getCatalog().setColumnPosition( catalogColumn.id, targetPosition );
                        }
                    }
                } else {
                    // Do nothing
                }
            } else if ( defaultValue != null ) {
                // TODO: String is only a temporal solution for default values
                String v = defaultValue.toString();
                if ( v.startsWith( "'" ) ) {
                    v = v.substring( 1, v.length() - 1 );
                }
                transaction.getCatalog().setDefaultValue( catalogColumn.id, PolySqlType.VARCHAR, v );
            } else if ( dropDefault != null && dropDefault ) {
                transaction.getCatalog().deleteDefaultValue( catalogColumn.id );
            } else {
                throw new RuntimeException( "Unknown option" );
            }
        } catch ( GenericCatalogException | UnknownEncodingException | UnknownTypeException | UnknownCollationException e ) {
            throw new RuntimeException( e );
        }
    }

}
