package com.evermind.tools.schemaupdate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.ResourcePatternResolver;

import com.evermind.tools.schemaupdate.liquibase.DatabaseChangeLogLoader;
import com.evermind.tools.schemaupdate.liquibase.SingleSpringResourceAccessor;

import liquibase.changelog.DatabaseChangeLog;

/**
 * Spring-Variante des Schema-Updaters
 */
public class SpringDbSchemaUpdater extends AbstractDbSchemaUpdater implements InitializingBean
{
    @Autowired
    protected ResourcePatternResolver resourcePatternResolver;
    
    protected String schemaUpdateResourcePattern;
    
    protected List<DatabaseChangeLog> additionalChangeLogs=new ArrayList<>();
    
    public void addAdditionalChangeLog(DatabaseChangeLog additionalChangeLog)
    {
        additionalChangeLogs.add(additionalChangeLog);
    }

    @Required
    public void setSchemaUpdateResourcePattern(String schemaUpdateResourcePattern)
    {
        this.schemaUpdateResourcePattern=schemaUpdateResourcePattern; 
    }
    
    protected Resource[] schemaUpdateResources;

    @Override
    public void afterPropertiesSet() throws Exception
    {
        loadDatabaseChangeLog();
    }
    
    @Override
    void internalLoadDatabaseChangeLog(DatabaseChangeLogLoader loader) throws Exception
    {
        for (DatabaseChangeLog changeLog: additionalChangeLogs)
        {
            loader.addChangeLog(changeLog);
        }
        
        if (schemaUpdateResourcePattern.startsWith(ResourcePatternResolver.CLASSPATH_URL_PREFIX) ||
                schemaUpdateResourcePattern.startsWith(ResourcePatternResolver.CLASSPATH_ALL_URL_PREFIX))
        {
            // convert path to package name
            String path=schemaUpdateResourcePattern.substring(schemaUpdateResourcePattern.indexOf(':')+1);
            path=path.substring(0, path.lastIndexOf('/'));
            path=path.replace('/', '.');
            loader.setCustomClassLookupPackage(path);
        }
        
        
        Map<String,Resource> resourcesByName=new TreeMap<>();
        // Resourcen finden und nach Namen aufsteigend sortieren
        for (Resource res: resourcePatternResolver.getResources(schemaUpdateResourcePattern))
        {
            resourcesByName.put(res.getFilename(), res);
        }
        
        for (Resource res: resourcesByName.values())
        {
            loader.addResource(res.getFilename(),new SingleSpringResourceAccessor(res));
        }
        
    }
    
    
//    public void updateSchema(LocalContainerEntityManagerFactoryBean entityManagerFactoryBean) throws SQLException
//    {
//        context.getResources(locationPattern)
//        
//        updateSchema(entityManagerFactoryBean.getDataSource().getConnection());
////        try (Connection con=entityManagerFactoryBean.getDataSource().getConnection())
////        {
////            DatabaseSnapshot snapshot=LiqibaseHelper.createDatabaseSnapshot(con);
////            LiqibaseHelper.printChangelog(snapshot, null, new XMLChangeLogSerializer(), System.err);
////        }
////        catch (Exception ex)
////        {
////            throw new RuntimeException(ex);
////        }
//        
////        
//////        EntityManagerFactoryImpl emf = (EntityManagerFactoryImpl) entityManagerFactoryBean.getNativeEntityManagerFactory();
//////        SessionFactoryImpl sf = (SessionFactoryImpl)emf.getSessionFactory();
//////        StandardServiceRegistry serviceRegistry = sf.getSessionFactoryOptions().getServiceRegistry();
//////        MetadataSources metadataSources = new MetadataSources(new BootstrapServiceRegistryBuilder().build());
//////        
//////        Configuration config=new Configuration(metadataSources);
//////        Metadata metadata = metadataSources.buildMetadata(serviceRegistry);
////        Configuration config = new Ejb3Configuration().configure(entityManagerFactoryBean.getPersistenceUnitInfo(), entityManagerFactoryBean.getJpaPropertyMap()).getHibernateConfiguration();
////        
////        Database referenceDatabase=new HibernateEjb3Database() {
////            @Override
////            protected Configuration buildConfiguration(HibernateConnection connection) throws DatabaseException
////            {
////                return ((LocalHibernateConnection)connection).getCfg();
////            }
////        };
////        referenceDatabase.setConnection(new JdbcConnection(new LocalHibernateConnection(config)));
////        
////        Database targetDatabase=new MySQLDatabase();
////        targetDatabase.setConnection(new JdbcConnection(entityManagerFactoryBean.getDataSource().getConnection()));
////
////        DatabaseChangeLog emptyChangeLog=new DatabaseChangeLog();
////        Liquibase liquibase = new Liquibase(emptyChangeLog,new MockResourceAccessor(),referenceDatabase);
////
////        CatalogAndSchema targetCatalogAndSchema = new CatalogAndSchema(targetDatabase.getDefaultCatalogName(), targetDatabase.getDefaultSchemaName());
////        CatalogAndSchema referenceCatalogAndSchema = new CatalogAndSchema(targetDatabase.getDefaultCatalogName(), targetDatabase.getDefaultSchemaName());
////        CompareControl.SchemaComparison[] schemaComparisons = {
////                new CompareControl.SchemaComparison(referenceCatalogAndSchema, targetCatalogAndSchema)
////        };
////
////        SnapshotGeneratorFactory snapshotGeneratorFactory = SnapshotGeneratorFactory.getInstance();
////        DatabaseSnapshot referenceSnapshot;
////        try {
////            referenceSnapshot = snapshotGeneratorFactory.createSnapshot(referenceDatabase.getDefaultSchema(),
////                    referenceDatabase, new SnapshotControl(referenceDatabase));
////        } catch (LiquibaseException e) {
////            throw new SQLException("Unable to create a DatabaseSnapshot. " + e.toString(), e);
////        }
////
////        CompareControl compareControl = new CompareControl(schemaComparisons, referenceSnapshot.getSnapshotControl().getTypesToInclude());
////
////        try {
////            DiffResult diffResult=liquibase.diff(referenceDatabase, targetDatabase, new CompareControl());
////            
////            DiffOutputControl diffOutputControl = new DiffOutputControl(false, false, false);
////            DiffToChangeLog diffToChangeLog = new DiffToChangeLog(diffResult, diffOutputControl);
////            
////            ChangeLogSerializer changeLogSerializer=new XMLChangeLogSerializer();
////            
////            diffToChangeLog.print(System.err, changeLogSerializer);
////            
////            
//////            DiffToReport diffReport = new DiffToReport(diffResult, System.out);
//////            diffReport.print();
////            System.out.flush();
////        } catch (Exception e) {
////            throw new SQLException("Unable to diff databases. " + e.toString(), e);
////        }
////        
////        
//////        
//////        
//////        PersistenceUnitInfo persistenceUnitInfo=entityManagerFactoryBean.getPersistenceUnitInfo();
//////        Map<String, Object> jpaProperties=entityManagerFactoryBean.getJpaPropertyMap();
//////        
//////        // http://hillert.blogspot.de/2010/05/using-hibernates-schemaexport-feature.html
//////        Configuration cfg = new Ejb3Configuration().configure(entityManagerFactoryBean.getPersistenceUnitInfo(), entityManagerFactoryBean.getJpaPropertyMap()).getHibernateConfiguration();
//////        
//////        Dialect dialect=Dialect.getDialect(cfg.getProperties());
//////        String defaultCatalog = cfg.getProperty( Environment.DEFAULT_CATALOG );
//////        String defaultSchema = cfg.getProperty( Environment.DEFAULT_SCHEMA );
//////        
//////        try (Connection db=entityManagerFactoryBean.getDataSource().getConnection())
//////        {
//////            DatabaseMetadata meta = new DatabaseMetadata( db, dialect, cfg);
//////            
//////            cfg.generateSchemaUpdateScriptList(dialect,meta); // Ensure all is set up
//////            
//////            for (Iterator<Table> iTables = cfg.getTableMappings();iTables.hasNext();)
//////            {
//////                Table table=iTables.next();
//////                String tableSchema = ( table.getSchema() == null ) ? defaultSchema : table.getSchema();
//////                String tableCatalog = ( table.getCatalog() == null ) ? defaultCatalog : table.getCatalog();
//////                if ( table.isPhysicalTable() ) {
//////
//////                    TableMetadata tableInfo = meta.getTableMetadata( table.getName(), tableSchema, tableCatalog, table.isQuoted() );
//////                    if ( tableInfo == null )
//////                    {
//////                        // Create new table
//////                        scripts.add( new SchemaUpdateScript( table.sqlCreateString( dialect, mapping, tableCatalog,
//////                                tableSchema ), false ) );
//////                    }
//////                    else {
//////                        Iterator<String> subiter = table.sqlAlterStrings( dialect, mapping, tableInfo, tableCatalog,
//////                                tableSchema );
//////                        while ( subiter.hasNext() ) {
//////                            scripts.add( new SchemaUpdateScript( subiter.next(), false ) );
//////                        }
//////                    }
//////
//////                    Iterator<String> comments = table.sqlCommentStrings( dialect, defaultCatalog, defaultSchema );
//////                    while ( comments.hasNext() ) {
//////                        scripts.add( new SchemaUpdateScript( comments.next(), false ) );
//////                    }
//////
//////                }
//////                
//////            }
//////            
//////            
//////            
//////            
//////        }
//////        
//////        
//////        
//    }
}
