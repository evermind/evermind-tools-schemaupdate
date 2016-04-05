package com.evermind.tools.schemaupdate.export;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Set;

import javax.sql.DataSource;
import javax.xml.parsers.ParserConfigurationException;

import org.hibernate.cfg.Configuration;
import org.hibernate.ejb.Ejb3Configuration;
import org.hibernate.ejb.EntityManagerFactoryImpl;
import org.hibernate.internal.SessionFactoryImpl;
import org.hibernate.service.internal.SessionFactoryServiceRegistryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;

import com.evermind.tools.schemaupdate.liquibase.LiqibaseHelper;
import com.evermind.tools.schemaupdate.liquibase.LocalHibernateConnection;
import com.evermind.tools.schemaupdate.liquibase.TablenameFilter;

import liquibase.database.Database;
import liquibase.database.jvm.JdbcConnection;
import liquibase.diff.DiffResult;
import liquibase.diff.output.DiffOutputControl;
import liquibase.diff.output.changelog.DiffToChangeLog;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.ext.hibernate.database.HibernateEjb3Database;
import liquibase.ext.hibernate.database.connection.HibernateConnection;
import liquibase.serializer.core.xml.XMLChangeLogSerializer;
import liquibase.snapshot.DatabaseSnapshot;

/**
 * http://stackoverflow.com/questions/34612019/programmatic-schemaexport-schemaupdate-with-hibernate-5-and-spring-4
 * http://hillert.blogspot.de/2010/05/using-hibernates-schemaexport-feature.html
 * http://stackoverflow.com/questions/32780664/hibernate-migration-from-4-3-x-to-5-x-for-method-org-hibernate-cfg-configuration
 * http://blog.essential-bytes.de/flyway-hibernate-und-jpa-integrieren/
 * 
 * @author mwyraz
 */
public class SpringOrmSchemaExporter
{
    protected final Logger LOG=LoggerFactory.getLogger(getClass()); 

    protected File schemaUpdateOutputFile;
    protected Set<String> ignoredTables=new HashSet<>();
    
    public void exportSchemaAndUpdates(Database referenceDatabase, Database actualDatabase) throws SQLException, LiquibaseException, IOException, ParserConfigurationException
    {
        exportSchemaAndUpdates(LiqibaseHelper.createDatabaseSnapshot(referenceDatabase),LiqibaseHelper.createDatabaseSnapshot(actualDatabase));
    }
    
    public void exportSchemaAndUpdates(Database referenceDatabase, Connection actualDatabaseConnection) throws SQLException, LiquibaseException, IOException, ParserConfigurationException
    {
        exportSchemaAndUpdates(referenceDatabase,LiqibaseHelper.getLiquibaseDatabase(actualDatabaseConnection));
    }
    
    public void exportSchemaAndUpdates(Database referenceDatabase, DataSource actualDatabase) throws SQLException, LiquibaseException, IOException, ParserConfigurationException
    {
        try (Connection actualDatabaseConnection=actualDatabase.getConnection())
        {
            exportSchemaAndUpdates(referenceDatabase,actualDatabaseConnection);
        }
    }
    
    public void exportSchemaAndUpdates(DatabaseSnapshot referenceState, DatabaseSnapshot actualDatabase) throws SQLException, LiquibaseException, IOException, ParserConfigurationException
    {
        DiffResult diff=LiqibaseHelper.createDiff(referenceState, actualDatabase);
        
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(baos);
        
        DiffOutputControl diffOutputControl=new DiffOutputControl(false,false,false);
        if (!ignoredTables.isEmpty())
        {
            diffOutputControl.setObjectChangeFilter(new TablenameFilter(ignoredTables));
        }
        DiffToChangeLog diffToChangeLog = new DiffToChangeLog(diff, diffOutputControl);
        diffToChangeLog.setIdRoot(new SimpleDateFormat("yyyyMMdd-01").format(System.currentTimeMillis()));
        
        if (diffToChangeLog.generateChangeSets().isEmpty())
        {
            LOG.debug("No outstanding schemaupdates found.");
            return;
        }
        
        diffToChangeLog.print(out, new XMLChangeLogSerializer());
        String schemaUpdates=baos.toString("utf-8");
        
        if (schemaUpdateOutputFile==null)
        {
            LOG.warn("Outstanding schemaupdates:\n{}",schemaUpdates);
        }
    }

    public void exportSchemaAndUpdates(LocalContainerEntityManagerFactoryBean entityManagerFactoryBean) throws SQLException, LiquibaseException, IOException, ParserConfigurationException
    {
        exportSchemaAndUpdates(entityManagerFactoryBean,null);
    }
    
    
    public void exportSchemaAndUpdates(LocalContainerEntityManagerFactoryBean entityManagerFactoryBean, Connection connectionToUse) throws SQLException, LiquibaseException, IOException, ParserConfigurationException
    {
        try
        {
            LOG.debug("Creating schema from hibernate mapping");
            EntityManagerFactoryImpl emf = (EntityManagerFactoryImpl) entityManagerFactoryBean.getNativeEntityManagerFactory();
            SessionFactoryImpl sf = emf.getSessionFactory();
            SessionFactoryServiceRegistryImpl serviceRegistry = (SessionFactoryServiceRegistryImpl) sf.getServiceRegistry();
            Configuration config = new Ejb3Configuration().configure(entityManagerFactoryBean.getPersistenceUnitInfo(), entityManagerFactoryBean.getJpaPropertyMap()).getHibernateConfiguration();
            
            Database referenceDatabase=new HibernateEjb3Database() {
                @Override
                protected Configuration buildConfiguration(HibernateConnection connection) throws DatabaseException
                {
                    return ((LocalHibernateConnection)connection).getCfg();
                }
            };
            referenceDatabase.setConnection(new JdbcConnection(new LocalHibernateConnection(config)));
            
            if (connectionToUse==null)
            {
                exportSchemaAndUpdates(referenceDatabase,entityManagerFactoryBean.getDataSource());
            }
            else
            {
                exportSchemaAndUpdates(referenceDatabase,connectionToUse);
            }
        }
        catch (Exception ex)
        {
            LOG.warn("Failed to check schema",ex);
        }
    }
 
    public void addIgnoredTables(String...tables)
    {
        for (String table: tables)
        {
            ignoredTables.add(table);
        }
    }

}
