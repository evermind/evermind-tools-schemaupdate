package com.evermind.tools.schemaupdate.liquibase;

import java.sql.Connection;

import liquibase.change.custom.CustomTaskChange;
import liquibase.database.Database;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.CustomChangeException;
import liquibase.exception.SetupException;
import liquibase.exception.ValidationErrors;
import liquibase.resource.ResourceAccessor;

public abstract class AbstractCustomChangeTask implements CustomTaskChange
{
    @Override
    public void execute(Database database) throws CustomChangeException
    {
        Connection connection=((JdbcConnection)database.getConnection()).getUnderlyingConnection();
        try
        {
            execute(database, connection);
        }
        catch (Exception ex)
        {
            throw new CustomChangeException(ex);
        }
    }
    
    protected abstract void execute(Database database, Connection connection) throws Exception;
    
    @Override
    public ValidationErrors validate(Database database)
    {
        return null;
    }
    
    @Override
    public void setUp() throws SetupException
    {
    }
    
    @Override
    public String getConfirmationMessage()
    {
        return getClass().getSimpleName()+" done.";
    }
    
    protected ResourceAccessor resourceAccessor;
    
    @Override
    public void setFileOpener(ResourceAccessor resourceAccessor)
    {
        this.resourceAccessor=resourceAccessor;
    }
}
