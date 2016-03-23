package com.evermind.tools.schemaupdate.liquibase;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import liquibase.database.Database;
import liquibase.diff.ObjectDifferences;
import liquibase.diff.output.ObjectChangeFilter;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Column;
import liquibase.structure.core.Data;
import liquibase.structure.core.ForeignKey;
import liquibase.structure.core.Index;
import liquibase.structure.core.PrimaryKey;
import liquibase.structure.core.Table;
import liquibase.structure.core.UniqueConstraint;

/**
 * A Liquibase filter that filters out everything that belongs to a list of
 * table names: - Tables - Foreign keys of this tables - Indices and COntraints
 * on this tables
 * 
 * @author mwyraz
 *
 */
public class TablenameFilter implements ObjectChangeFilter
{
    protected final Set<String> ignoredTables;

    public TablenameFilter(Collection<String> ignoredTables)
    {
        this.ignoredTables = new HashSet<>(ignoredTables);
    }

    @Override
    public boolean includeChanged(DatabaseObject object, ObjectDifferences differences, Database referenceDatabase, Database comparisionDatabase)
    {
        if (ignoredTables.isEmpty()) return true;
        return include(object, referenceDatabase, comparisionDatabase);
    }

    @Override
    public boolean includeMissing(DatabaseObject object, Database referenceDatabase, Database comparisionDatabase)
    {
        if (ignoredTables.isEmpty()) return true;
        return include(object, referenceDatabase, comparisionDatabase);
    }

    @Override
    public boolean includeUnexpected(DatabaseObject object, Database referenceDatabase, Database comparisionDatabase)
    {
        if (ignoredTables.isEmpty()) return true;
        return include(object, referenceDatabase, comparisionDatabase);
    }

    protected boolean include(DatabaseObject object, Database referenceDatabase, Database comparisionDatabase)
    {

        if (object instanceof Table)
        {
            return !(ignoredTables.contains(object.getName()));
        }

        if (object instanceof Column)
        {
            return include(((Column) object).getRelation(), referenceDatabase, comparisionDatabase);
        }
        if (object instanceof ForeignKey)
        {
            return include(((ForeignKey) object).getForeignKeyTable(),referenceDatabase, comparisionDatabase);
        }
        if (object instanceof Index)
        {
            return include(((Index) object).getTable(),referenceDatabase, comparisionDatabase);
        }
        if (object instanceof PrimaryKey)
        {
            return include(((PrimaryKey) object).getTable(),referenceDatabase, comparisionDatabase);
        }
        if (object instanceof Data)
        {
            return include(((Data) object).getTable(),referenceDatabase, comparisionDatabase);
        }
        if (object instanceof UniqueConstraint)
        {
            return include(((UniqueConstraint) object).getTable(),referenceDatabase, comparisionDatabase);
        }

        return true;
    }

}
