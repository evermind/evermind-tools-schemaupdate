package com.evermind.tools.schemaupdate.export;

import java.io.IOException;
import java.sql.SQLException;

import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;

import liquibase.database.Database;
import liquibase.exception.LiquibaseException;

public interface IHibernateAdapter
{
    public Database getHibernateDatabaseFromSpring(LocalContainerEntityManagerFactoryBean entityManagerFactoryBean) throws SQLException, LiquibaseException, IOException;
}
