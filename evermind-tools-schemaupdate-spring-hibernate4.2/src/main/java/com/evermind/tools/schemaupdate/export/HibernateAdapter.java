package com.evermind.tools.schemaupdate.export;

import org.hibernate.cfg.Configuration;
import org.hibernate.ejb.Ejb3Configuration;
import org.hibernate.ejb.EntityManagerFactoryImpl;
import org.hibernate.internal.SessionFactoryImpl;
import org.hibernate.service.internal.SessionFactoryServiceRegistryImpl;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;

import liquibase.database.Database;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.ext.hibernate.database.HibernateEjb3Database;
import liquibase.ext.hibernate.database.connection.HibernateConnection;

public class HibernateAdapter implements IHibernateAdapter
{
    @Override
    public Database getHibernateDatabaseFromSpring(LocalContainerEntityManagerFactoryBean entityManagerFactoryBean)
    {
        EntityManagerFactoryImpl emf = (EntityManagerFactoryImpl) entityManagerFactoryBean.getNativeEntityManagerFactory();
        SessionFactoryImpl sf = emf.getSessionFactory();
        SessionFactoryServiceRegistryImpl serviceRegistry = (SessionFactoryServiceRegistryImpl) sf.getServiceRegistry();
        Configuration config = new Ejb3Configuration().configure(entityManagerFactoryBean.getPersistenceUnitInfo(), entityManagerFactoryBean.getJpaPropertyMap()).getHibernateConfiguration();
        
        Database database=new HibernateEjb3Database() {
            @Override
            protected Configuration buildConfiguration(HibernateConnection connection) throws DatabaseException
            {
                return ((LocalHibernateConnection)connection).getCfg();
            }
        };
        database.setConnection(new JdbcConnection(new LocalHibernateConnection(config)));
        
        return database;
    }
    
}
