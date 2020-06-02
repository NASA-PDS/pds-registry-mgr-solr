package gov.nasa.pds.registry.mgr.cmd;

import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.cli.CommandLine;
import org.apache.solr.client.solrj.SolrClient;

import gov.nasa.pds.registry.mgr.Constants;
import gov.nasa.pds.registry.mgr.util.SolrUtils;

public class SetArchiveStatusCmd implements CliCommand
{
    private static final String DEFAULT_STATUS_NAME = "STAGED";
    private Set<String> STATUS_NAME; 

    private String collectionName;
    private String status;
    private String lidvid;
    private String packageId;
    
    
    public SetArchiveStatusCmd()
    {
        STATUS_NAME = new TreeSet<>();

        STATUS_NAME.add("ARCHIVED");
        STATUS_NAME.add("ARCHIVED_ACCUMULATING");
        STATUS_NAME.add("IN_LIEN_RESOLUTION");
        STATUS_NAME.add("IN_LIEN_RESOLUTION_ACCUMULATING");
        STATUS_NAME.add("IN_PEER_REVIEW");
        STATUS_NAME.add("IN_PEER_REVIEW_ACCUMULATING");
        STATUS_NAME.add("IN_QUEUE");
        STATUS_NAME.add("IN_QUEUE_ACCUMULATING");
        STATUS_NAME.add("LOCALLY_ARCHIVED");
        STATUS_NAME.add("LOCALLY_ARCHIVED_ACCUMULATING");
        STATUS_NAME.add("PRE_PEER_REVIEW");
        STATUS_NAME.add("PRE_PEER_REVIEW_ACCUMULATING");
        STATUS_NAME.add("SAFED");
        STATUS_NAME.add("STAGED");
        STATUS_NAME.add("SUPERSEDED");
    }
    
    
    @Override
    public void run(CommandLine cmdLine) throws Exception
    {
        if(cmdLine.hasOption("help"))
        {
            printHelp();
            return;
        }

        // Read and validate parameters
        this.collectionName = cmdLine.getOptionValue("collection", Constants.DEFAULT_REGISTRY_COLLECTION);
        if(!getStatus(cmdLine)) return;
        if(getIds(cmdLine)) return;

        // Update status
        SolrClient client = SolrUtils.createSolrClient(cmdLine);        
        updateStatus(client);
    }

    
    private boolean getStatus(CommandLine cmdLine)
    {
        String tmp = cmdLine.getOptionValue("status");
        if(tmp == null) 
        {
            System.out.println("ERROR: Missing required parameter '-status'");
            System.out.println();
            printHelp();
            return false;
        }

        this.status = tmp.toUpperCase();
        if(!STATUS_NAME.contains(status))
        {
            System.out.println("ERROR: Invalid '-status' parameter value: '" + tmp + "'");
            System.out.println();
            printHelp();
            return false;
        }
        
        return true;
    }

    
    private boolean getIds(CommandLine cmdLine)
    {
        this.lidvid = cmdLine.getOptionValue("lidvid");
        this.packageId = cmdLine.getOptionValue("packageId");

        if(lidvid == null && packageId == null)
        {
            System.out.println("ERROR: Either '-lidvid' or '-packageId' parameter is required");
            System.out.println();
            printHelp();
            return false;
        }

        if(lidvid != null && packageId != null)
        {
            System.out.println("ERROR: Could not have both '-lidvid' and '-packageId' parameters");
            System.out.println();
            printHelp();
            return false;
        }

        return true;
    }
    

    private void updateStatus(SolrClient client)
    {
        if(lidvid != null)
        {
            updateStatusByLidvid(client);
        }
        else if(packageId != null)
        {
            updateStatusByPackageId(client);
        }
    }
    
    
    private void updateStatusByLidvid(SolrClient client)
    {
        
    }
    

    private void updateStatusByPackageId(SolrClient client)
    {
        
    }
    
    
    public void printHelp()
    {
        System.out.println("Usage: registry-manager set-archive-status <options>");

        System.out.println();
        System.out.println("Set product archive status");
        System.out.println();
        System.out.println("Required parameters:");
        System.out.println("  -status <status>    One of the following values:");

        for(String name: STATUS_NAME)
        {
            System.out.println("      " + name);
        }
        
        System.out.println("  -lidvid <id>        Update archive status of a document with given lidvid, or");
        System.out.println("  -packageId <id>     Update archive status of all documents with given package id"); 
        System.out.println("Optional parameters:");
        System.out.println("  -solrUrl <url>      Solr URL. Default is http://localhost:8983/solr");
        System.out.println("  -zkHost <host>      ZooKeeper connection string, <host:port>[,<host:port>][/path]");
        System.out.println("                      For example, zk1:2181,zk2:2181,zk3:2181/solr");
        System.out.println("  -collection <name>  Solr collection name. Default value is 'registry'");
        System.out.println();
    }

}
