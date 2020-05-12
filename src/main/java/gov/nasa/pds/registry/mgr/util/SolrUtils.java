package gov.nasa.pds.registry.mgr.util;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.cli.CommandLine;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.ZkClientClusterStateProvider;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient.RemoteSolrException;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;


public class SolrUtils
{
    public static SolrClient createSolrClient(CommandLine cmdLine)
    {
        Logger LOG = LogManager.getLogger(SolrUtils.class);
        SolrClient client = null;
        
        String zkHost = cmdLine.getOptionValue("zkHost");
        if(zkHost == null)
        {
            String solrUrl = cmdLine.getOptionValue("solrUrl", "http://localhost:8983/solr");
            client = new HttpSolrClient.Builder(solrUrl).build();
            LOG.info("Solr URL: " + solrUrl);
        }
        else
        {
            ZkClientClusterStateProvider zk = new ZkClientClusterStateProvider(zkHost);
            client = new CloudSolrClient.Builder(zk).build();
            LOG.info("Solr Zookeeper: " + zkHost);
        }

        return client;
    }
    
    
    public static Set<String> getFieldNames(SolrClient client, String collectionName) throws Exception
    {
        try
        {
            SchemaRequest.Fields req = new SchemaRequest.Fields();
            SchemaResponse.FieldsResponse resp = req.process(client, collectionName);
    
            List<Map<String, Object>> fields = resp.getFields();
            Set<String> names = new TreeSet<>();
            
            for(Map<String, Object> field: fields)
            {
                String fieldName = (String)field.get("name");
                names.add(fieldName);
            }
            
            return names;
        }
        catch(RemoteSolrException ex)
        {
            int code = ex.code();
            switch(code)
            {
            case 401: throw new Exception("Not authenticated.");
            case 404: throw new Exception("Collection '" + collectionName + "' not found.");
            }
            
            throw ex;
        }
    }
    
    
    public static SchemaRequest.AddField createAddFieldRequest(String fieldName, String fieldType)
    {
        Map<String, Object> params = new TreeMap<>();
        params.put("name", fieldName);
        params.put("type", fieldType);
        params.put("stored", true);
        params.put("indexed", true);
        params.put("multiValued", true);
        
        SchemaRequest.AddField req = new SchemaRequest.AddField(params);
        return req;
    }

}
