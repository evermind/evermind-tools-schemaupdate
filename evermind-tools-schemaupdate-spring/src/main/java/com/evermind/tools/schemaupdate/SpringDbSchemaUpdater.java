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
        loadDatabaseChangeLogs();
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
}
