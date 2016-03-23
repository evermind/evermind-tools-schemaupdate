package com.evermind.tools.schemaupdate.liquibase;

import org.hibernate.cfg.Configuration;

import liquibase.ext.hibernate.database.connection.HibernateConnection;

public class LocalHibernateConnection extends HibernateConnection
{
    final Configuration cfg;
    
    public LocalHibernateConnection(Configuration cfg)
    {
        super("hibernate:local");
        this.cfg=cfg;
    }
    public Configuration getCfg()
    {
        return cfg;
    }
}
