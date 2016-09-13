package com.evermind.tools.schemaupdate.liquibase;

import java.util.ArrayList;
import java.util.List;

import liquibase.change.ChangeFactory;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.exception.ChangeLogParseException;
import liquibase.parser.core.xml.XMLChangeLogSAXParser;
import liquibase.resource.ResourceAccessor;

public class DatabaseChangeLogLoader
{
    protected final List<DatabaseChangeLog> databaseChangeLogs;
    
    public DatabaseChangeLogLoader()
    {
        ChangeFactory.getInstance().unregister("customChange");
        ChangeFactory.getInstance().register(ExtendedCustomChangeWrapper.class);
        
        this.databaseChangeLogs=new ArrayList<>();
    }
    
    public void setCustomClassLookupPackage(String packageName)
    {
        ExtendedCustomChangeWrapper.setLookupPackage(packageName);
    }
    
    public List<DatabaseChangeLog> getDatabaseChangeLogs()
    {
        return databaseChangeLogs;
    }

    public void addChangeLog(DatabaseChangeLog changeLog) throws ChangeLogParseException
    {
        this.databaseChangeLogs.add(changeLog);
    }
    
    public void addXmlResource(String filename, ResourceAccessor accessor) throws ChangeLogParseException
    {
        XMLChangeLogSAXParser parser=new XMLChangeLogSAXParser();
        addChangeLog(parser.parse(filename, null, accessor));
    }
    
    public void addResource(String filename, ResourceAccessor accessor) throws ChangeLogParseException
    {
        if (filename.toLowerCase().endsWith(".xml"))
        {
            addXmlResource(filename, accessor);
            return;
        }
        throw new RuntimeException("Resource type not supported: "+filename);
    }

}
