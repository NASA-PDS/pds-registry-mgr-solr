package gov.nasa.pds.registry.mgr.util;

import org.apache.commons.cli.CommandLine;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.ZkClientClusterStateProvider;

public class SolrUtils
{
    public static SolrClient createSolrClient(CommandLine cmdLine)
    {
        SolrClient client = null;
        
        String zkHost = cmdLine.getOptionValue("zkHost");
        if(zkHost == null)
        {
            String solrUrl = cmdLine.getOptionValue("solrUrl", "http://localhost:8983/solr");
            client = new HttpSolrClient.Builder(solrUrl).build();
        }
        else
        {
            ZkClientClusterStateProvider zk = new ZkClientClusterStateProvider(zkHost);
            client = new CloudSolrClient.Builder(zk).build();        
        }

        return client;
    }
}
