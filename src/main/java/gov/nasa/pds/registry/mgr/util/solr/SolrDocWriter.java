package gov.nasa.pds.registry.mgr.util.solr;

import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

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
        
        
        
        writer.append("</doc>\n");
    }
}
