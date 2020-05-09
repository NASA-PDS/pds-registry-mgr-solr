package gov.nasa.pds.registry.mgr.cmd;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.ZkClientClusterStateProvider;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;

import gov.nasa.pds.registry.mgr.Constants;
import gov.nasa.pds.registry.mgr.util.CloseUtils;


public class CreateRegistryCmd implements CliCommand
{
    public CreateRegistryCmd()
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
        File configDir = getConfigDir(cmdLine.getOptionValue("configDir"));
        
        String collectionName = cmdLine.getOptionValue("collection", Constants.DEFAULT_REGISTRY_COLLECTION);
        int shards = parseShards(cmdLine.getOptionValue("shards", "1"));
        int replicas = parseReplicas(cmdLine.getOptionValue("replicas", "1"));
        
        System.out.println("Creating registry collection");
        System.out.println();
        System.out.println("ZooKeeper host: " + zkHost);
        System.out.println("Configuration directory: " + configDir.getAbsolutePath());
        System.out.println("Collection: " + collectionName);
        System.out.println("Shards: " + shards);
        System.out.println("Replicas: " + replicas);
        System.out.println();

        
        ZkClientClusterStateProvider zk = null;
        CloudSolrClient client = null;
        try
        {
            System.out.println("Uploading configuration...");
            zk = new ZkClientClusterStateProvider(zkHost);
            zk.uploadConfig(configDir.toPath(), collectionName);
    
            System.out.println("Creating collection...");
            client = new CloudSolrClient.Builder(zk).build();
            CollectionAdminRequest.Create req = CollectionAdminRequest.Create
                    .createCollection(collectionName, collectionName, 1, 1);

            @SuppressWarnings("unused")
            CollectionAdminResponse resp = req.process(client);
            System.out.println("Done");
        }
        finally
        {
            CloseUtils.close(client);
            CloseUtils.close(zk);
        }
    }

    
    private int parseShards(String str) throws Exception
    {
        int val = parseInt(str);
        if(val == 0) throw new Exception("Invalid number of shards: " + str);
        
        return val;
    }
    

    private int parseReplicas(String str) throws Exception
    {
        int val = parseInt(str);
        if(val == 0) throw new Exception("Invalid number of replicas: " + str);
        
        return val;
    }

    
    private int parseInt(String str)
    {
        if(str == null) return 0;
        
        try
        {
            return Integer.parseInt(str);
        }
        catch(Exception ex)
        {
            return 0;
        }
    }
    
    
    private File getConfigDir(String configDirParam) throws Exception
    {
        File dir = null;
        
        if(configDirParam == null)
        {
            // Get default
            String home = System.getenv("REGISTRY_MANAGER_HOME");
            if(home == null) 
            {
                throw new Exception("Could not find default configuration directory. REGISTRY_MANAGER_HOME environment variable is not set.");
            }
            
            dir = new File(home, "solr/collections/registry");
        }
        else
        {
            dir = new File(configDirParam);
        }
        
        if(!dir.exists()) throw new Exception("Directory " + dir.getAbsolutePath() + " does not exist");
        if(!dir.isDirectory()) throw new Exception("Not a directory: " + dir.getAbsolutePath());
        
        return dir;
    }
    
    
    public void printHelp()
    {
        System.out.println("Usage: registry-manager create-registry <options>");

        System.out.println();
        System.out.println("Create registry collection");
        System.out.println();
        System.out.println("Optional parameters:");
        System.out.println("  -zkHost <host>      ZooKeeper connection string, <host:port>[,<host:port>][/path]");
        System.out.println("                      For example, zk1:2181,zk2:2181,zk3:2181/solr"); 
        System.out.println("                      Default value is localhost:9983");
        System.out.println("  -configDir <dir>    Configuration directory with registry collection configuration files"); 
        System.out.println("                      Default value is $REGISTRY_MANAGER_HOME/solr/collections/registry");
        System.out.println("  -collection <name>  Solr collection name. Default value is 'registry'");
        System.out.println("  -shards <number>    Number of shards for registry collection. Default value is 1");
        System.out.println("  -replicas <number>  Number of replicas for registry collection. Default value is 1");
        System.out.println();
    }

}
