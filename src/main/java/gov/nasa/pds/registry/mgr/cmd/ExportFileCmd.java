package gov.nasa.pds.registry.mgr.cmd;

import org.apache.commons.cli.CommandLine;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;

import gov.nasa.pds.registry.mgr.Constants;
import gov.nasa.pds.registry.mgr.util.CloseUtils;
import gov.nasa.pds.registry.mgr.util.EmbeddedBlobExporter;
import gov.nasa.pds.registry.mgr.util.SolrUtils;

public class ExportFileCmd implements CliCommand
{
    public ExportFileCmd()
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
        
        // Lidvid
        String lidvid = cmdLine.getOptionValue("lidvid");
        if(lidvid == null) 
        {
            System.out.println("ERROR: Missing required parameter '-lidvid'");
            System.out.println();
            printHelp();
            return;
        }
        
        // File path
        String filePath = cmdLine.getOptionValue("filePath");
        if(filePath == null) 
        {
            System.out.println("ERROR: Missing required parameter '-filePath'");
            System.out.println();
            printHelp();
            return;
        }

        SolrClient client = SolrUtils.createSolrClient(cmdLine);
        SolrQuery solrQuery = new SolrQuery("lidvid:\"" + lidvid + "\"");
        solrQuery.set("fl", "_file_blob");
        
        try
        {
            // Get Solr doc by lidvid
            QueryResponse resp = client.query(Constants.REGISTRY_COLLECTION, solrQuery);
            long numDocs = resp.getResults().getNumFound();
            if(numDocs == 0)
            {
                System.out.println("Could not find a document with lidvid = " + lidvid);
                return;
            }
            
            // Get the blob. It can be empty.
            SolrDocument doc = resp.getResults().get(0);
            Object obj = doc.getFieldValue("_file_blob");
            if(obj == null)
            {
                System.out.println("There is no blob in a document with lidvid = " + lidvid);
                System.out.println("Probably embedded blob storage was not enabled when the document was created.");
                return;
            }

            // Export blob
            byte[] blob = (byte[])obj;
            System.out.println("Exporting blob " + lidvid + " to " + filePath);
            EmbeddedBlobExporter exporter = new EmbeddedBlobExporter();
            exporter.export(blob, filePath);
            System.out.println("Done");
        }
        finally
        {
            CloseUtils.close(client);
        }
    }

    
    public void printHelp()
    {
        System.out.println("Usage: registry-manager export-file <options>");

        System.out.println();
        System.out.println("Export a file from blob storage");
        System.out.println();
        System.out.println("Required parameters:");
        System.out.println("  -lidvid <id>      Lidvid of a file to export from blob storage.");
        System.out.println("  -filePath <path>  A path to a file to write."); 
        System.out.println("Optional parameters:");
        System.out.println("  -solrUrl <url>    Solr URL. Default is http://localhost:8983/solr");
        System.out.println("  -zkHost <host>    ZooKeeper connection string, <host:port>[,<host:port>][/path]");
        System.out.println("                    For example, zk1:2181,zk2:2181,zk3:2181/solr"); 
        System.out.println();
    }

}
