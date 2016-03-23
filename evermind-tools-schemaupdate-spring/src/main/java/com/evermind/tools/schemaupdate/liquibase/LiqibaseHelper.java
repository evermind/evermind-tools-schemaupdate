package com.evermind.tools.schemaupdate.liquibase;

import java.io.IOException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;

import liquibase.CatalogAndSchema;
import liquibase.changelog.ChangeSet;
import liquibase.database.Database;
import liquibase.database.core.H2Database;
import liquibase.database.core.MySQLDatabase;
import liquibase.database.jvm.JdbcConnection;
import liquibase.diff.DiffGeneratorFactory;
import liquibase.diff.DiffResult;
import liquibase.diff.compare.CompareControl;
import liquibase.diff.output.DiffOutputControl;
import liquibase.diff.output.changelog.DiffToChangeLog;
import liquibase.exception.LiquibaseException;
import liquibase.serializer.ChangeLogSerializer;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.SnapshotControl;
import liquibase.snapshot.SnapshotGeneratorFactory;

public class LiqibaseHelper
{
    public static Database getLiquibaseDatabase(Connection connection) throws LiquibaseException, SQLException
    {
        DatabaseMetaData meta = connection.getMetaData();
        String driver = meta.getDriverName();
        String url = meta.getURL();

        if (url.startsWith("jdbc:mysql:"))
        {
            return connect(new MySQLDatabase(), connection);
        }
        if (url.startsWith("jdbc:h2:"))
        {
            return connect(new H2Database(), connection);
        }

        throw new LiquibaseException("Unable to determine database type for " + driver + " / " + url);
    }

    protected static Database connect(Database database, Connection connection)
    {
        database.setConnection(new JdbcConnection(connection));
        return database;
    }

    public static DatabaseSnapshot createDatabaseSnapshot(Connection connection) throws LiquibaseException, SQLException
    {
        return createDatabaseSnapshot(getLiquibaseDatabase(connection));
    }
    public static DatabaseSnapshot createDatabaseSnapshot(Database db) throws LiquibaseException, SQLException
    {
        CatalogAndSchema catalogAndSchema = new CatalogAndSchema(db.getDefaultCatalogName(), db.getDefaultSchemaName());
        SnapshotControl snapshotControl = new SnapshotControl(db);
        return SnapshotGeneratorFactory.getInstance().createSnapshot(catalogAndSchema, db, snapshotControl);
    }

    public static DiffResult createDiff(DatabaseSnapshot referenceState, DatabaseSnapshot actualState) throws LiquibaseException, SQLException
    {
        return DiffGeneratorFactory.getInstance().compare(referenceState, actualState, new CompareControl());
    }

    public static List<ChangeSet> createChangeSets(DatabaseSnapshot referenceState, DatabaseSnapshot actualState) throws LiquibaseException, SQLException
    {
        DiffResult diff = createDiff(referenceState, actualState);

        DiffToChangeLog diffToChangeLog = new DiffToChangeLog(diff, new DiffOutputControl(false,false,false));
        return diffToChangeLog.generateChangeSets();
    }

    public static void printChangelog(DatabaseSnapshot referenceState, DatabaseSnapshot actualState, ChangeLogSerializer changeLogSerializer, PrintStream out) throws LiquibaseException, SQLException, ParserConfigurationException, IOException
    {
        printChangelog(createDiff(referenceState, actualState), changeLogSerializer, out);
    }

    public static void printChangelog(DiffResult diff, ChangeLogSerializer changeLogSerializer, PrintStream out) throws LiquibaseException, SQLException, ParserConfigurationException, IOException
    {
        DiffToChangeLog diffToChangeLog = new DiffToChangeLog(diff, new DiffOutputControl(false,false,false));
        diffToChangeLog.setIdRoot(new SimpleDateFormat("yyyyMMdd-01").format(System.currentTimeMillis()));
        diffToChangeLog.print(out, changeLogSerializer);
    }
    
}
