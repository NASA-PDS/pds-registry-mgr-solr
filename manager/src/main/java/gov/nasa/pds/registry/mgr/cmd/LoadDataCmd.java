package gov.nasa.pds.registry.mgr.cmd;

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.ZkClientClusterStateProvider;
import org.apache.solr.client.solrj.request.ContentStreamUpdateRequest;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.tika.Tika;

import gov.nasa.pds.registry.mgr.Constants;
import gov.nasa.pds.registry.mgr.util.CloseUtils;


public class LoadDataCmd implements CliCommand
{
    private final static String XML_CTX_TYPE = "application/xml";

    public LoadDataCmd()
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

        // Get list of files to load
        String filePath = cmdLine.getOptionValue("filePath");
        if(filePath == null) throw new Exception("Missing required parameter '-filePath'");
        
        List<File> files = getFiles(filePath);
        if(files == null || files.isEmpty()) return;
        
        // Create Solr connection
        ZkClientClusterStateProvider zk = null;
        SolrClient client = null;
        
        String zkHost = cmdLine.getOptionValue("zkHost");
        if(zkHost == null)
        {
            String solrUrl = cmdLine.getOptionValue("solrUrl", "http://localhost:8983/solr");
            client = new HttpSolrClient.Builder(solrUrl).build();
        }
        else
        {
            zk = new ZkClientClusterStateProvider(zkHost);
            client = new CloudSolrClient.Builder(zk).build();        
        }
        
        try
        {
            for(File file: files)
            {
                loadFile(client, file);
            }
            
            System.out.println("Done");
        }
        finally
        {
            CloseUtils.close(client);
            CloseUtils.close(zk);
        }
    }

    
    private static class XmlFileFilter implements FileFilter
    {
        private Tika tika;
        
        public XmlFileFilter()
        {
            tika = new Tika();
        }
        
        @Override
        public boolean accept(File file)
        {
            if(!file.isFile()) return false;

            try
            {
                String type = tika.detect(file);
                if(!"application/xml".equals(type)) return false;
            }
            catch(Exception ex)
            {
                return false;
            }
            
            return true;
        }
    }

    
    private List<File> getFiles(String filePath) throws Exception
    {
        File file = new File(filePath);

        if(file.isDirectory())
        {
            File[] ff = file.listFiles(new XmlFileFilter());
            if(ff == null || ff.length == 0)
            {
                System.out.println("Could not find any XML files in " + file.getAbsolutePath());
                return null;
            }
            
            return Arrays.asList(ff);
        }
        else
        {
            // Check if the file exists
            if(!file.exists()) throw new Exception("File does not exist: " + file.getAbsolutePath());
            
            // Check content type. Only XML is supported.
            Tika tika = new Tika();
            String type = tika.detect(file);
            if(!XML_CTX_TYPE.equals(type)) throw new Exception("Unsupported context type " + type 
                    + " of file " + file);
        
            List<File> list = new ArrayList<>(1);
            list.add(file);
            return list;
        }
    }
    
    
    private void loadFile(SolrClient client, File file) throws Exception
    {
        System.out.println("Loading file: " + file.getAbsolutePath());
        
        ContentStreamUpdateRequest req = new ContentStreamUpdateRequest("/update");
        req.setParam(CollectionAdminParams.COLLECTION, Constants.REGISTRY_COLLECTION);
        req.setMethod(SolrRequest.METHOD.POST);
        req.addFile(file, XML_CTX_TYPE);

        req.process(client);
        client.commit(Constants.REGISTRY_COLLECTION);
    }
    
    
    public void printHelp()
    {
        System.out.println("Usage: registry-manager load-data <options>");

        System.out.println();
        System.out.println("Load data into registry collection");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  -filePath <path>    An XML file or a directory to load. This is a required parameter."); 
        System.out.println("  -solrUrl <url>      Solr URL. Default is http://localhost:8983/solr");
        System.out.println("  -zkHost <host>      ZooKeeper connection string, <host:port>[,<host:port>][/path]");
        System.out.println("                      For example, zk1:2181,zk2:2181,zk3:2181/solr"); 
        System.out.println();
    }

}
