package gov.nasa.pds.registry.mgr.cmd;

import org.apache.commons.cli.CommandLine;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.ZkClientClusterStateProvider;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.ConfigSetAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.ConfigSetAdminResponse;

import gov.nasa.pds.registry.mgr.Constants;
import gov.nasa.pds.registry.mgr.util.CloseUtils;
import gov.nasa.pds.registry.mgr.util.ExceptionUtils;


public class DeleteRegistryCmd implements CliCommand
{
    public DeleteRegistryCmd()
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
        
        String zkHost = cmdLine.getOptionValue("zkHost", "localhost:9983");
        String collectionName = cmdLine.getOptionValue("collection", Constants.DEFAULT_REGISTRY_COLLECTION);

        System.out.println("ZooKeeper host: " + zkHost);

        ZkClientClusterStateProvider zk = null;
        CloudSolrClient client = null;
        try
        {
            zk = new ZkClientClusterStateProvider(zkHost);
            client = new CloudSolrClient.Builder(zk).build();
            
            deleteCollection(client, collectionName);
            deleteConfigSet(client, collectionName);
        }
        finally
        {
            CloseUtils.close(client);
            CloseUtils.close(zk);
        }
    }

    
    private void deleteCollection(CloudSolrClient client, String collectionName)
    {
        System.out.println("Deleting collection " + collectionName + "...");
        CollectionAdminRequest.Delete req = CollectionAdminRequest.Delete.deleteCollection(collectionName);
        
        try
        {
            @SuppressWarnings("unused")
            CollectionAdminResponse resp = req.process(client);
        }
        catch(Exception ex)
        {
            System.out.println("WARNING: Could not delete registry collection: "  + ExceptionUtils.getMessage(ex));
        }
    }
    

    private void deleteConfigSet(CloudSolrClient client, String collectionName)
    {
        System.out.println("Deleting configset " + collectionName + "...");
        ConfigSetAdminRequest.Delete req = new ConfigSetAdminRequest.Delete();
        req.setConfigSetName(collectionName);
        
        try
        {
            @SuppressWarnings("unused")
            ConfigSetAdminResponse resp = req.process(client);
            System.out.println("Done");
        }
        catch(Exception ex)
        {
            System.out.println("WARNING: Could not delete registry configset: "  + ExceptionUtils.getMessage(ex));
        }
    }

    
    public void printHelp()
    {
        System.out.println("Usage: registry-manager delete-registry <options>");

        System.out.println();
        System.out.println("Delete registry collection and all its data");
        System.out.println();
        System.out.println("Optional parameters:");
        System.out.println("  -zkHost <host>      ZooKeeper connection string, <host:port>[,<host:port>][/path]");
        System.out.println("                      For example, zk1:2181,zk2:2181,zk3:2181/solr"); 
        System.out.println("                      Default value is localhost:9983");
        System.out.println("  -collection <name>  Solr collection name. Default value is 'registry'");
    }

}
