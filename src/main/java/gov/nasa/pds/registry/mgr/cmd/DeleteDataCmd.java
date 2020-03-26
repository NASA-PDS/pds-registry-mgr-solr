package gov.nasa.pds.registry.mgr.cmd;

import org.apache.commons.cli.CommandLine;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;

import gov.nasa.pds.registry.mgr.Constants;
import gov.nasa.pds.registry.mgr.util.CloseUtils;
import gov.nasa.pds.registry.mgr.util.SolrUtils;


public class DeleteDataCmd implements CliCommand
{
    public DeleteDataCmd()
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

        String query = buildSolrQuery(cmdLine);
        if(query == null)
        {
            System.out.println("ERROR: One of the following options is required: -lidvid, -lid, -packageId, -all");
            System.out.println();
            printHelp();
            return;
        }
        
        SolrClient client = SolrUtils.createSolrClient(cmdLine);
        
        try
        {
            // Find out, how many records will be deleted. 
            // I could not find a way to get number of deleted docs from a delete query.
            SolrQuery solrQuery = new SolrQuery(query);
            solrQuery.setRows(0);
            QueryResponse resp = client.query(Constants.REGISTRY_COLLECTION, solrQuery);
            long numDocs = resp.getResults().getNumFound();
            
            // Run delete command
            if(numDocs > 0)
            {
                client.deleteByQuery(Constants.REGISTRY_COLLECTION, query);
                client.commit(Constants.REGISTRY_COLLECTION);
            }
            
            System.out.println("Deleted " + numDocs + " document(s)");
        }
        finally
        {
            CloseUtils.close(client);
        }
    }

    
    private String buildSolrQuery(CommandLine cmdLine)
    {
        String id = cmdLine.getOptionValue("lidvid");
        if(id != null)
        {
            return "lidvid:\"" + id + "\"";
        }
        
        id = cmdLine.getOptionValue("lid");
        if(id != null)
        {
            return "lid:\"" + id + "\"";
        }

        id = cmdLine.getOptionValue("packageId");
        if(id != null)
        {
            return "_package_id:\"" + id + "\"";
        }

        if(cmdLine.hasOption("all"))
        {
            return "*:*";
        }
        
        return null;
    }
    
    
    public void printHelp()
    {
        System.out.println("Usage: registry-manager delete-data <options>");

        System.out.println();
        System.out.println("Delete data from registry collection");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  -lidvid <id>     Delete data by lidvid");
        System.out.println("  -lid <id>        Delete data by lid");
        System.out.println("  -packageId <id>  Delete data by package id"); 
        System.out.println("  -all             Delete all data");
        System.out.println("  -solrUrl <url>   Solr URL. Default is http://localhost:8983/solr");
        System.out.println("  -zkHost <host>   ZooKeeper connection string, <host:port>[,<host:port>][/path]");
        System.out.println("                   For example, zk1:2181,zk2:2181,zk3:2181/solr"); 
        System.out.println();
    }

}
