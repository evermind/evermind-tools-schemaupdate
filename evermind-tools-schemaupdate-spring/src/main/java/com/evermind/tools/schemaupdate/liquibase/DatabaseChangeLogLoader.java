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

    public void addChangeLog(DatabaseChangeLog changeLog) throws ChangeLogParseException
    {
        if (changeLog==null) return;
        
        boolean changeLogHasPreconditions=(changeLog.getPreconditions()!=null && changeLog.getPreconditions().getNestedPreconditions().size()>0);
        
        for (ChangeSet changeSet : changeLog.getChangeSets())
        {
            if (changeLogHasPreconditions) // Copy global preconditions to all change sets of this change log
            {
                if (changeSet.getPreconditions()==null || changeSet.getPreconditions().getNestedPreconditions().size()==0)
                {
                    // overwrite if there are no existing
                    changeSet.setPreconditions(changeLog.getPreconditions());
                }
                else
                {
                    // append to existing preconditions
                    changeSet.getPreconditions().addNestedPrecondition(changeLog.getPreconditions());
                }
            }
            
            databaseChangeLog.addChangeSet(changeSet);
        }
    }
    
    public void addXmlResource(String filename, ResourceAccessor accessor) throws ChangeLogParseException
    {
	XMLChangeLogSAXParserWithLocalSchemaFiles parser=new XMLChangeLogSAXParserWithLocalSchemaFiles();
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
