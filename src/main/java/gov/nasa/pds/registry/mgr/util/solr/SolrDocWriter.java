package gov.nasa.pds.registry.mgr.util.solr;

import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Date;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;


public class SolrDocWriter implements Closeable
{
    private Writer writer;

    
    public SolrDocWriter(File file) throws Exception
    {
        writer = new FileWriter(file);
        writer.append("<add>\n");
    }

    
    @Override
    public void close() throws IOException
    {
        writer.append("</add>\n");
        writer.close();
    }

    
    public void write(SolrDocumentList docs) throws Exception
    {
        if(docs == null) return;
        
        for(SolrDocument doc: docs)
        {
            write(doc);
        }
    }
    
    
    private void write(SolrDocument doc) throws Exception
    {
        writer.append("<doc>\n");
        
        for(String fieldName: doc.getFieldNames())
        {
            Collection<Object> values = doc.getFieldValues(fieldName);
            if(values != null && values.size() > 0)
            {
                for(Object value: values)
                {
                    writeField(fieldName, value);
                }
            }
        }
        
        writer.append("</doc>\n");
    }


    private void writeField(String key, Object value) throws Exception
    {
        if(key == null || value == null || "_version_".equals(key)) return;
        
        writer.write("  <field name=\"");
        writer.write(key);
        writer.write("\">");
        
        String strValue;
        if(value instanceof Date)
        {
            strValue = DateTimeFormatter.ISO_INSTANT.format(((Date)value).toInstant());
        }
        else
        {
            strValue = value.toString();
        }
        
        writer.write(StringEscapeUtils.escapeXml(strValue));
        
        writer.write("</field>\n");
    }
    
}
