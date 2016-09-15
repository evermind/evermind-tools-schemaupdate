package com.evermind.tools.schemaupdate;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evermind.tools.schemaupdate.liquibase.DatabaseChangeLogLoader;
import com.evermind.tools.schemaupdate.liquibase.LiqibaseHelper;

import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.database.Database;

/**
 * Schema-Updater für Hibernate-Konfigurationen:
 * - Der Hibernate-Dialect wird für die Updates verwendet
 * - optional wird Hibernate für die Erzeugung von Deltas verwendet 
 * @author mwyraz
 *
 * http://stackoverflow.com/questions/34612019/programmatic-schemaexport-schemaupdate-with-hibernate-5-and-spring-4
 * http://hillert.blogspot.de/2010/05/using-hibernates-schemaexport-feature.html
 * http://stackoverflow.com/questions/32780664/hibernate-migration-from-4-3-x-to-5-x-for-method-org-hibernate-cfg-configuration
 * http://blog.essential-bytes.de/flyway-hibernate-und-jpa-integrieren/
 */
public abstract class AbstractDbSchemaUpdater
{
    protected final Logger LOG=LoggerFactory.getLogger(getClass()); 
    
    protected Contexts contexts=new Contexts("default");
    
    public void setContexts(String... contexts)
    {
        this.contexts = new Contexts(contexts);
    }
    
    protected DatabaseChangeLog databaseChangeLog;
    protected synchronized DatabaseChangeLog loadDatabaseChangeLog() throws Exception
    {
        if (databaseChangeLog==null)
        {
            DatabaseChangeLogLoader loader=new DatabaseChangeLogLoader();
            internalLoadDatabaseChangeLog(loader);
            this.databaseChangeLog=loader.getDatabaseChangeLog();
        }
        return databaseChangeLog;
    }
    
    abstract void internalLoadDatabaseChangeLog(DatabaseChangeLogLoader loader) throws Exception;

    public void updateSchema(DataSource ds) throws SQLException
    {
        updateSchema(ds,null);
    }
    
    public void updateSchema(DataSource ds, String optionalSchemaName) throws SQLException
    {
        try (Connection con=ds.getConnection())
        {
            updateSchema(con, optionalSchemaName);
        }
    }
    public void updateSchema(Connection con, String optionalSchemaName) throws SQLException
    {
        try
        {
            LOG.debug("Updating schema");
            DatabaseChangeLog changelog=loadDatabaseChangeLog();
            
            Database db=LiqibaseHelper.getLiquibaseDatabase(con);
            
            if (optionalSchemaName!=null) db.setDefaultSchemaName(optionalSchemaName);
            
            Liquibase liquibase=new Liquibase(changelog, null, db);
            liquibase.update(contexts);
        }
        catch (Exception ex)
        {
            throw new SQLException(ex);
        }
    }   
}
