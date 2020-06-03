package gov.nasa.pds.registry.mgr.cmd;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.cli.CommandLine;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;

import gov.nasa.pds.registry.mgr.Constants;
import gov.nasa.pds.registry.mgr.util.CloseUtils;
import gov.nasa.pds.registry.mgr.util.SolrUtils;

public class SetArchiveStatusCmd implements CliCommand
{
    private Set<String> statusNames; 

    private String collectionName;
    private String status;
    private String lidvid;
    private String packageId;
    
    
    public SetArchiveStatusCmd()
    {
        statusNames = new TreeSet<>();

        statusNames.add("ARCHIVED");
        statusNames.add("ARCHIVED_ACCUMULATING");
        statusNames.add("IN_LIEN_RESOLUTION");
        statusNames.add("IN_LIEN_RESOLUTION_ACCUMULATING");
        statusNames.add("IN_PEER_REVIEW");
        statusNames.add("IN_PEER_REVIEW_ACCUMULATING");
        statusNames.add("IN_QUEUE");
        statusNames.add("IN_QUEUE_ACCUMULATING");
        statusNames.add("LOCALLY_ARCHIVED");
        statusNames.add("LOCALLY_ARCHIVED_ACCUMULATING");
        statusNames.add("PRE_PEER_REVIEW");
        statusNames.add("PRE_PEER_REVIEW_ACCUMULATING");
        statusNames.add("SAFED");
        statusNames.add("STAGED");
        statusNames.add("SUPERSEDED");
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
        if(!getIds(cmdLine)) return;

        // Update status
        SolrClient client = SolrUtils.createSolrClient(cmdLine);
        System.out.println("[INFO] Solr collection: " + collectionName);
        updateStatus(client);
    }

    
    private boolean getStatus(CommandLine cmdLine)
    {
        String tmp = cmdLine.getOptionValue("status");
        if(tmp == null) 
        {
            System.out.println("[ERROR] Missing required parameter '-status'");
            System.out.println();
            printHelp();
            return false;
        }

        this.status = tmp.toUpperCase();
        if(!statusNames.contains(status))
        {
            System.out.println("[ERROR] Invalid '-status' parameter value: '" + tmp + "'");
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
            System.out.println("[ERROR] Either '-lidvid' or '-packageId' parameter is required");
            System.out.println();
            printHelp();
            return false;
        }

        if(lidvid != null && packageId != null)
        {
            System.out.println("[ERROR] Could not have both '-lidvid' and '-packageId' parameters");
            System.out.println();
            printHelp();
            return false;
        }

        return true;
    }
    

    private void updateStatus(SolrClient client) throws Exception
    {
        try
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
        finally
        {
            CloseUtils.close(client);
        }
    }
    
    
    private void updateStatusByLidvid(SolrClient client) throws Exception
    {
        System.out.println("[INFO] Updating arhive status. Lidvid = " + lidvid + ", status = " + status);
        
        // Check that the lidvid exists        
        SolrQuery solrQuery = new SolrQuery("lidvid:\"" + lidvid + "\"");
        solrQuery.set("fl", "lidvid");

        // Get Solr doc by lidvid
        QueryResponse resp = client.query(collectionName, solrQuery);
        long numDocs = resp.getResults().getNumFound();
        if(numDocs == 0)
        {
            System.out.println("[ERROR] Could not find a document with lidvid = " + lidvid);
            return;
        }
        
        // Update document
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("lidvid", lidvid);
        
        Map<String, String> map = new TreeMap<>();
        map.put("set", status);
        doc.addField("archive_status", map);

        List<SolrInputDocument> docs = new ArrayList<>();
        docs.add(doc);
        
        client.add(this.collectionName, docs);
        client.commit(this.collectionName);
        System.out.println("[INFO] Done");
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

        for(String name: statusNames)
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
