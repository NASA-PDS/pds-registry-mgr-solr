package gov.nasa.pds.registry.mgr.cmd;

import org.apache.commons.cli.CommandLine;

import gov.nasa.pds.registry.mgr.Constants;

public class SetArchiveStatusCmd implements CliCommand
{
    private static String[] STATUS_NAME = 
    {
        "ARCHIVED",
        "ARCHIVED_ACCUMULATING",
        "IN_LIEN_RESOLUTION",
        "IN_LIEN_RESOLUTION_ACCUMULATING",
        "IN_PEER_REVIEW",
        "IN_PEER_REVIEW_ACCUMULATING",
        "IN_QUEUE",
        "IN_QUEUE_ACCUMULATING",
        "LOCALLY_ARCHIVED",
        "LOCALLY_ARCHIVED_ACCUMULATING",
        "PRE_PEER_REVIEW",
        "PRE_PEER_REVIEW_ACCUMULATING",
        "SAFED",
        "STAGED",
        "SUPERSEDED"
    };

    
    public SetArchiveStatusCmd()
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
        
        // Collection name
        String collectionName = cmdLine.getOptionValue("collection", Constants.DEFAULT_REGISTRY_COLLECTION);
        
        // Status
        String status = cmdLine.getOptionValue("status");
        if(status == null) 
        {
            System.out.println("ERROR: Missing required parameter '-status'");
            System.out.println();
            printHelp();
            return;
        }

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
