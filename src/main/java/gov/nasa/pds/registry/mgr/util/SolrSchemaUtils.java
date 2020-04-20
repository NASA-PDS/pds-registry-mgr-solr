package gov.nasa.pds.registry.mgr.util;

import java.io.Writer;

public class SolrSchemaUtils
{
    public static void writeSchemaField(Writer writer, String fieldName, String dataType) throws Exception
    {
        writer.write("<field");
        writeAttribute(writer, "name", fieldName);
        writeAttribute(writer, "type", dataType);
        writeAttribute(writer, "indexed", "true");
        writeAttribute(writer, "stored", "true");
        writeAttribute(writer, "multiValued", "true");
        writeAttribute(writer, "required", "false");
        writer.write(" />\n");
    }
    
    
    public static void writeAttribute(Writer writer, String name, String value) throws Exception
    {
        writer.write(" " + name + "=\"" + value + "\"");
    }
}
