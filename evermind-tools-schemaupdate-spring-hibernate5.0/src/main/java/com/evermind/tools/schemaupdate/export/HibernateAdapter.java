package com.evermind.tools.schemaupdate.export;

import java.util.Iterator;

import javax.persistence.EntityManagerFactory;
import javax.persistence.metamodel.ManagedType;

import org.hibernate.boot.Metadata;
import org.hibernate.boot.MetadataBuilder;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.service.ServiceRegistry;
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
		final EntityManagerFactory emf = entityManagerFactoryBean.getNativeEntityManagerFactory();

		// use class from liquibase-hibernate5-project
		// but it can only be configured via factory class or config file
		// so we override buildMetadata() and use existing EntityManagerFactory
		Database database = new HibernateEjb3Database()
		{
			@Override
			protected Metadata buildMetadata(HibernateConnection connection) throws DatabaseException
			{
				String dialectString = (String) emf.getProperties().get(AvailableSettings.DIALECT);

				ServiceRegistry standardRegistry = new StandardServiceRegistryBuilder().applySetting(AvailableSettings.DIALECT, configureDialect(dialectString)).build();

				MetadataSources sources = new MetadataSources(standardRegistry);

				Iterator<ManagedType<?>> it = emf.getMetamodel().getManagedTypes().iterator();
				while (it.hasNext())
				{
					Class<?> javaType = it.next().getJavaType();
					if (javaType == null)
					{
						continue;
					}
					sources.addAnnotatedClass(javaType);
				}

				Package[] packages = Package.getPackages();
				for (Package p : packages)
				{
					sources.addPackage(p);
				}

				MetadataBuilder metadataBuilder = sources.getMetadataBuilder();
				metadataBuilder.enableNewIdentifierGeneratorSupport(true);
				configureNewIdentifierGeneratorSupport(metadataBuilder, (String) emf.getProperties().get(AvailableSettings.USE_NEW_ID_GENERATOR_MAPPINGS));
				configureImplicitNamingStrategy(metadataBuilder, (String) emf.getProperties().get(AvailableSettings.IMPLICIT_NAMING_STRATEGY));
				configurePhysicalNamingStrategy(metadataBuilder, (String) emf.getProperties().get(AvailableSettings.PHYSICAL_NAMING_STRATEGY));

				return metadataBuilder.build();
			}
		};
		database.setConnection(new JdbcConnection(new HibernateConnection("hibernate:ejb3:dummy")));
		
		return database;
	}

}
