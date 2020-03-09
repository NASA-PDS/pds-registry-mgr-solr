package gov.nasa.pds.registry.mgr.util;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.solr.client.solrj.impl.CloudSolrClient;

public class SolrCloudUtils
{
    
    public static CloudSolrClient getDefaultSolrClient()
    {
        return getSolrClient("localhost:9983", "/");
    }
    
    
    public static CloudSolrClient getSolrClient(String zkHosts, String zkPath)
    {
        if(zkHosts == null) throw new IllegalArgumentException("ZooKeeper host is null");
        
        String[] strHosts = zkHosts.split("[,;]");
        List<String> hosts = Arrays.asList(strHosts);

        CloudSolrClient.Builder bld = new CloudSolrClient.Builder(hosts, Optional.of(zkPath));
        CloudSolrClient client = bld.build();
        return client;
    }

}
