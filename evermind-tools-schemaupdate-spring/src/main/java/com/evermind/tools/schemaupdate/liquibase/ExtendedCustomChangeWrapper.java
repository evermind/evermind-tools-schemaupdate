package com.evermind.tools.schemaupdate.liquibase;

import liquibase.change.custom.CustomChangeWrapper;
import liquibase.parser.core.ParsedNode;
import liquibase.parser.core.ParsedNodeException;
import liquibase.resource.ResourceAccessor;
import liquibase.util.file.FilenameUtils;

/**
 * Special handling for custom changes:
 * - if the class name is "", and the the resource's name will be used as classname
 * - if the classname is "" or starts with ".", the lookup package
 * @author mwyraz
 *
 */
public class ExtendedCustomChangeWrapper extends CustomChangeWrapper
{
    protected static String lookupPackage;
    
    public static void setLookupPackage(String lookupPackage)
    {
        ExtendedCustomChangeWrapper.lookupPackage = lookupPackage;
    }
    
    @Override
    public void load(ParsedNode parsedNode, ResourceAccessor resourceAccessor) throws ParsedNodeException
    {
        ParsedNode classNode=parsedNode.getChild(null, "class");
        String className=classNode.getValue(String.class);
        
        if (className.isEmpty())
        {
            if (resourceAccessor instanceof INamedResourceAccessor)
            {
                String resourceName=((INamedResourceAccessor) resourceAccessor).getResourceName();
                className=FilenameUtils.getBaseName(resourceName);
                if (lookupPackage!=null) className=lookupPackage+"."+className;
            }
            else
            {
                throw new RuntimeException("No classname given and the resource accessor is not an INamedResourceAccessor.");
            }
        }
        else if (className.startsWith("."))
        {
            className=lookupPackage+"."+className;
        }
        classNode.setValue(className);
        
        super.load(parsedNode, resourceAccessor);
    }
}
