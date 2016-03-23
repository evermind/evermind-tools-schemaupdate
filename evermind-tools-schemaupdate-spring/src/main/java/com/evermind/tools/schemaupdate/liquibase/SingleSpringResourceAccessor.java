package com.evermind.tools.schemaupdate.liquibase;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Set;

import org.springframework.core.io.Resource;

import liquibase.resource.ResourceAccessor;

/**
 * Accessor that serves exactly one spring resource by the given name
 * @author mwyraz
 */
public class SingleSpringResourceAccessor implements ResourceAccessor, INamedResourceAccessor
{
    protected final String resourceName;
    protected final Resource resource;

    public SingleSpringResourceAccessor(Resource resource)
    {
        this(resource.getFilename(),resource);
    }
    
    public SingleSpringResourceAccessor(String resourceName, Resource resource)
    {
        this.resourceName=resourceName;
        this.resource=resource;
    }
    
    @Override
    public Set<InputStream> getResourcesAsStream(String path) throws IOException
    {
        if (resourceName.equals(path)) return Collections.singleton(resource.getInputStream());
        return null;
    }
    
    @Override
    public Set<String> list(String relativeTo, String path, boolean includeFiles, boolean includeDirectories, boolean recursive) throws IOException
    {
        throw new UnsupportedOperationException();
    }
    @Override
    public ClassLoader toClassLoader()
    {
        return getClass().getClassLoader();
    }
    
    public String getResourceName()
    {
        return resourceName;
    }
}
