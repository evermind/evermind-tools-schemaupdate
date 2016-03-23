package com.evermind.tools.schemaupdate.liquibase;

import liquibase.change.ChangeFactory;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.exception.ChangeLogParseException;
import liquibase.parser.core.xml.XMLChangeLogSAXParser;
import liquibase.resource.ResourceAccessor;

public class DatabaseChangeLogLoader
{
    protected final DatabaseChangeLog databaseChangeLog;
    
    public DatabaseChangeLogLoader()
    {
        ChangeFactory.getInstance().unregister("customChange");
        ChangeFactory.getInstance().register(ExtendedCustomChangeWrapper.class);
        
        this.databaseChangeLog=new DatabaseChangeLog();
    }
    
    public void setCustomClassLookupPackage(String packageName)
    {
        ExtendedCustomChangeWrapper.setLookupPackage(packageName);
    }
    
    public DatabaseChangeLog getDatabaseChangeLog()
    {
        return databaseChangeLog;
    }

    protected boolean supportsGlobalPreconditions;
    
    public void addChangeLog(DatabaseChangeLog changeLog) throws ChangeLogParseException
    {
        if (changeLog==null) return;
        
         
        // see liquibase.changelog.DatabaseChangeLog.include(String, boolean, ResourceAccessor)
        if (changeLog.getPreconditions()!=null && changeLog.getPreconditions().getNestedPreconditions().size()>0)
        {
            if (supportsGlobalPreconditions)
            {
                if (databaseChangeLog.getPreconditions()==null)
                {
                    databaseChangeLog.setPreconditions(changeLog.getPreconditions());
                }
                else
                {
                    databaseChangeLog.getPreconditions().addNestedPrecondition(changeLog.getPreconditions());
                }
            }
            else
            {
                throw new ChangeLogParseException("Global preconditions apply to ALL changesets (and ALL changelogs) - therfore global changesets are not supported.");
            }
        }
        for (ChangeSet changeSet : changeLog.getChangeSets())
        {
            databaseChangeLog.addChangeSet(changeSet);
        }
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
