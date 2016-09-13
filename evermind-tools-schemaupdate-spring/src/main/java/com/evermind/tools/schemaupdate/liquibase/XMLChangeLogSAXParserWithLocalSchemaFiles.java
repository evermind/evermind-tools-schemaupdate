package com.evermind.tools.schemaupdate.liquibase;

import java.lang.reflect.Field;
import java.net.URL;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;

import liquibase.parser.core.xml.XMLChangeLogSAXParser;

public class XMLChangeLogSAXParserWithLocalSchemaFiles extends XMLChangeLogSAXParser
{
    public XMLChangeLogSAXParserWithLocalSchemaFiles()
    {
	super();
	try
	{
	    Field _saxParserFactory=getClass().getSuperclass().getDeclaredField("saxParserFactory");
	    _saxParserFactory.setAccessible(true);
	    _saxParserFactory.set(this, new SAXParserFactoryWrapper(getSaxParserFactory()));
	}
	catch (Exception ex)
	{
	    throw new RuntimeException("This class is not compatible with your liquibase version.",ex);
	}
    }
    
    protected static SAXParser prepareSaxParser(SAXParser saxParser) throws SAXException
    {
	StringBuilder builder=new StringBuilder();
	addXsd(builder, "http://www.liquibase.org/xml/ns/dbchangelog", "xsd/dbchangelog-3.5-modified.xsd");
	addXsd(builder, "http://www.liquibase.org/xml/ns/dbchangelog-ext", "xsd/dbchangelog-ext.xsd");
	saxParser.setProperty("http://apache.org/xml/properties/schema/external-schemaLocation", builder.toString());
	return saxParser;
    }
    
    protected static void addXsd(StringBuilder builder, String namespace, String xsd)
    {
	URL res=XMLChangeLogSAXParserWithLocalSchemaFiles.class.getClassLoader().getResource(xsd);
	if (res==null) throw new RuntimeException("Missing classpath resource: "+xsd);
	if (builder.length()>0) builder.append("\n");
	builder.append(namespace).append(" ").append(res.toExternalForm());
    }
    
    static class SAXParserFactoryWrapper extends SAXParserFactory
    {
	final SAXParserFactory factory;
	public SAXParserFactoryWrapper(SAXParserFactory factory) {
	    this.factory=factory;
	}
	public SAXParser newSAXParser() throws ParserConfigurationException, SAXException {
	    return prepareSaxParser(factory.newSAXParser());
	}
	public void setFeature(String name, boolean value)
		throws ParserConfigurationException, SAXNotRecognizedException, SAXNotSupportedException {
	    factory.setFeature(name, value);
	}
	public boolean getFeature(String name)
		throws ParserConfigurationException, SAXNotRecognizedException, SAXNotSupportedException {
	    return factory.getFeature(name);
	}
    }

}
