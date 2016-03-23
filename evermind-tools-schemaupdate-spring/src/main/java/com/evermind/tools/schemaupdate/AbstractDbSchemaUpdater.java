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
        try (Connection con=ds.getConnection())
        {
            updateSchema(con);
        }
    }
    public void updateSchema(Connection con) throws SQLException
    {
        try
        {
            LOG.debug("Updating schema");
            DatabaseChangeLog changelog=loadDatabaseChangeLog();
            
            Liquibase liquibase=new Liquibase(changelog, null, LiqibaseHelper.getLiquibaseDatabase(con));
            liquibase.update(contexts);
        }
        catch (Exception ex)
        {
            throw new SQLException(ex);
        }
    }
    
        
//        
////        EntityManagerFactoryImpl emf = (EntityManagerFactoryImpl) entityManagerFactoryBean.getNativeEntityManagerFactory();
////        SessionFactoryImpl sf = (SessionFactoryImpl)emf.getSessionFactory();
////        StandardServiceRegistry serviceRegistry = sf.getSessionFactoryOptions().getServiceRegistry();
////        MetadataSources metadataSources = new MetadataSources(new BootstrapServiceRegistryBuilder().build());
////        
////        Configuration config=new Configuration(metadataSources);
////        Metadata metadata = metadataSources.buildMetadata(serviceRegistry);
//        Configuration config = new Ejb3Configuration().configure(entityManagerFactoryBean.getPersistenceUnitInfo(), entityManagerFactoryBean.getJpaPropertyMap()).getHibernateConfiguration();
//        
//        Database referenceDatabase=new HibernateEjb3Database() {
//            @Override
//            protected Configuration buildConfiguration(HibernateConnection connection) throws DatabaseException
//            {
//                return ((LocalHibernateConnection)connection).getCfg();
//            }
//        };
//        referenceDatabase.setConnection(new JdbcConnection(new LocalHibernateConnection(config)));
//        
//        Database targetDatabase=new MySQLDatabase();
//        targetDatabase.setConnection(new JdbcConnection(entityManagerFactoryBean.getDataSource().getConnection()));
//
//        DatabaseChangeLog emptyChangeLog=new DatabaseChangeLog();
//        Liquibase liquibase = new Liquibase(emptyChangeLog,new MockResourceAccessor(),referenceDatabase);
//
//        CatalogAndSchema targetCatalogAndSchema = new CatalogAndSchema(targetDatabase.getDefaultCatalogName(), targetDatabase.getDefaultSchemaName());
//        CatalogAndSchema referenceCatalogAndSchema = new CatalogAndSchema(targetDatabase.getDefaultCatalogName(), targetDatabase.getDefaultSchemaName());
//        CompareControl.SchemaComparison[] schemaComparisons = {
//                new CompareControl.SchemaComparison(referenceCatalogAndSchema, targetCatalogAndSchema)
//        };
//
//        SnapshotGeneratorFactory snapshotGeneratorFactory = SnapshotGeneratorFactory.getInstance();
//        DatabaseSnapshot referenceSnapshot;
//        try {
//            referenceSnapshot = snapshotGeneratorFactory.createSnapshot(referenceDatabase.getDefaultSchema(),
//                    referenceDatabase, new SnapshotControl(referenceDatabase));
//        } catch (LiquibaseException e) {
//            throw new SQLException("Unable to create a DatabaseSnapshot. " + e.toString(), e);
//        }
//
//        CompareControl compareControl = new CompareControl(schemaComparisons, referenceSnapshot.getSnapshotControl().getTypesToInclude());
//
//        try {
//            DiffResult diffResult=liquibase.diff(referenceDatabase, targetDatabase, new CompareControl());
//            
//            DiffOutputControl diffOutputControl = new DiffOutputControl(false, false, false);
//            DiffToChangeLog diffToChangeLog = new DiffToChangeLog(diffResult, diffOutputControl);
//            
//            ChangeLogSerializer changeLogSerializer=new XMLChangeLogSerializer();
//            
//            diffToChangeLog.print(System.err, changeLogSerializer);
//            
//            
////            DiffToReport diffReport = new DiffToReport(diffResult, System.out);
////            diffReport.print();
//            System.out.flush();
//        } catch (Exception e) {
//            throw new SQLException("Unable to diff databases. " + e.toString(), e);
//        }
//        
//        
////        
////        
////        PersistenceUnitInfo persistenceUnitInfo=entityManagerFactoryBean.getPersistenceUnitInfo();
////        Map<String, Object> jpaProperties=entityManagerFactoryBean.getJpaPropertyMap();
////        
////        // http://hillert.blogspot.de/2010/05/using-hibernates-schemaexport-feature.html
////        Configuration cfg = new Ejb3Configuration().configure(entityManagerFactoryBean.getPersistenceUnitInfo(), entityManagerFactoryBean.getJpaPropertyMap()).getHibernateConfiguration();
////        
////        Dialect dialect=Dialect.getDialect(cfg.getProperties());
////        String defaultCatalog = cfg.getProperty( Environment.DEFAULT_CATALOG );
////        String defaultSchema = cfg.getProperty( Environment.DEFAULT_SCHEMA );
////        
////        try (Connection db=entityManagerFactoryBean.getDataSource().getConnection())
////        {
////            DatabaseMetadata meta = new DatabaseMetadata( db, dialect, cfg);
////            
////            cfg.generateSchemaUpdateScriptList(dialect,meta); // Ensure all is set up
////            
////            for (Iterator<Table> iTables = cfg.getTableMappings();iTables.hasNext();)
////            {
////                Table table=iTables.next();
////                String tableSchema = ( table.getSchema() == null ) ? defaultSchema : table.getSchema();
////                String tableCatalog = ( table.getCatalog() == null ) ? defaultCatalog : table.getCatalog();
////                if ( table.isPhysicalTable() ) {
////
////                    TableMetadata tableInfo = meta.getTableMetadata( table.getName(), tableSchema, tableCatalog, table.isQuoted() );
////                    if ( tableInfo == null )
////                    {
////                        // Create new table
////                        scripts.add( new SchemaUpdateScript( table.sqlCreateString( dialect, mapping, tableCatalog,
////                                tableSchema ), false ) );
////                    }
////                    else {
////                        Iterator<String> subiter = table.sqlAlterStrings( dialect, mapping, tableInfo, tableCatalog,
////                                tableSchema );
////                        while ( subiter.hasNext() ) {
////                            scripts.add( new SchemaUpdateScript( subiter.next(), false ) );
////                        }
////                    }
////
////                    Iterator<String> comments = table.sqlCommentStrings( dialect, defaultCatalog, defaultSchema );
////                    while ( comments.hasNext() ) {
////                        scripts.add( new SchemaUpdateScript( comments.next(), false ) );
////                    }
////
////                }
////                
////            }
////            
////            
////            
////            
////        }
////        
////        
////        
}
