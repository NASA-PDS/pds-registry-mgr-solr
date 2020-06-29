package gov.nasa.pds.registry.mgr.cmd;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.SortClause;
import org.apache.solr.common.SolrDocumentList;

import gov.nasa.pds.registry.mgr.Constants;
import gov.nasa.pds.registry.mgr.util.CloseUtils;
import gov.nasa.pds.registry.mgr.util.solr.SolrCursor;
import gov.nasa.pds.registry.mgr.util.solr.SolrDocWriter;
import gov.nasa.pds.registry.mgr.util.solr.SolrUtils;


public class ExportDataCmd implements CliCommand
{
    private static final int BATCH_SIZE = 100;

    
    public ExportDataCmd()
    {
    }

    
    @Override
    public void run(CommandLine cmdLine) throws Exception
    {
        if(cmdLine.hasOption("help"))
        {
            printHelp();
            return;
        }

        String collectionName = cmdLine.getOptionValue("collection", Constants.DEFAULT_REGISTRY_COLLECTION);
        
        // File path
        String filePath = cmdLine.getOptionValue("file");
        if(filePath == null) 
        {
            System.out.println("ERROR: Missing required parameter '-file'");
            System.out.println();
            printHelp();
            return;
        }

        // Query
        String query = buildSolrQuery(cmdLine);
        if(query == null)
        {
            System.out.println("ERROR: One of the following options is required: -lidvid, -packageId");
            System.out.println();
            printHelp();
            return;
        }
        
        SolrDocWriter writer = new SolrDocWriter(new File(filePath));
        SolrClient client = SolrUtils.createSolrClient(cmdLine);
        
        try
        {
            SolrQuery solrQuery = new SolrQuery(query);

            // Sort is required by Solr cursor
            solrQuery.setSort(SortClause.asc("lidvid"));
            solrQuery.setRows(BATCH_SIZE);

            int numDocs = 0;
            
            SolrCursor cursor = new SolrCursor(client, collectionName, solrQuery);
            while(cursor.next())
            {
                SolrDocumentList docs = cursor.getResults();
                writer.write(docs);
                
                numDocs += docs.size();
                if(docs.size() != 0)
                {
                    System.out.println("Exported " + numDocs + " document(s)");
                }
            }
            
            System.out.println("Done");
        }
        finally
        {
            CloseUtils.close(client);
            CloseUtils.close(writer);
        }
    }

    
    private String buildSolrQuery(CommandLine cmdLine)
    {
        String id = cmdLine.getOptionValue("lidvid");
        if(id != null)
        {
            return "lidvid:\"" + id + "\"";
        }
        
        id = cmdLine.getOptionValue("packageId");
        if(id != null)
        {
            return "_package_id:\"" + id + "\"";
        }

        return null;
    }
    
    
    public void printHelp()
    {
        System.out.println("Usage: registry-manager export-data <options>");

        System.out.println();
        System.out.println("Export data from registry collection");
        System.out.println();
        System.out.println("Required parameters:");
        System.out.println("  -file <path>        Output file path");        
        System.out.println("  -lidvid <id>        Export data by lidvid");
        System.out.println("  -packageId <id>     Export data by package id"); 
        System.out.println("Optional parameters:");
        System.out.println("  -solrUrl <url>      Solr URL. Default is http://localhost:8983/solr");
        System.out.println("  -zkHost <host>      ZooKeeper connection string, <host:port>[,<host:port>][/path]");
        System.out.println("                      For example, zk1:2181,zk2:2181,zk3:2181/solr");
        System.out.println("  -collection <name>  Solr collection name. Default value is 'registry'");
        System.out.println();
    }

}
