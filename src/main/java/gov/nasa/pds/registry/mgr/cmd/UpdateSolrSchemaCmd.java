package gov.nasa.pds.registry.mgr.cmd;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;

import gov.nasa.pds.registry.mgr.Constants;
import gov.nasa.pds.registry.mgr.schema.SolrSchemaUpdater;
import gov.nasa.pds.registry.mgr.schema.cfg.ConfigReader;
import gov.nasa.pds.registry.mgr.schema.cfg.Configuration;
import gov.nasa.pds.registry.mgr.schema.dd.DataDictionary;
import gov.nasa.pds.registry.mgr.schema.dd.JsonDDParser;
import gov.nasa.pds.registry.mgr.util.SolrUtils;
import gov.nasa.pds.registry.mgr.util.log.RegistryLogManager;


public class UpdateSolrSchemaCmd implements CliCommand
{
    private Logger LOG;
    
    public UpdateSolrSchemaCmd()
    {
        // NOTE: Do not get logger here. It is not initialized yet!
    }
    
    
    @Override
    public void run(CommandLine cmdLine) throws Exception
    {
        // NOTE: Do not get logger in constructor or static initializer!
        LOG = LogManager.getLogger(getClass());
        
        if(cmdLine.hasOption("help"))
        {
            printHelp();
            return;
        }

        String cfgPath = cmdLine.getOptionValue("config");
        if(cfgPath == null) 
        {
            System.out.println("ERROR: Missing required parameter '-config'");
            System.out.println();
            printHelp();
            return;
        }
        
        // This logger does not depend on -v (verbosity) command line parameter.
        Logger minLogger = RegistryLogManager.getMinInfoLogger();
        
        // Read configuration file
        File cfgFile = new File(cfgPath);
        minLogger.info("Reading configuration from " + cfgFile.getAbsolutePath());
        ConfigReader cfgReader = new ConfigReader();
        Configuration cfg = cfgReader.read(cfgFile);
        
        SolrClient client = SolrUtils.createSolrClient(cmdLine);
        String solrCollection = cmdLine.getOptionValue("collection", Constants.DEFAULT_REGISTRY_COLLECTION);
        LOG.info("Solr collection: " + solrCollection);
        
        // Update Solr schema
        updateSolrSchema(cfg, client, solrCollection);
    }

    
    public void printHelp()
    {
        System.out.println("Usage: registry-manager update-solr-schema <options>");

        System.out.println();
        System.out.println("Update Solr schema from one or more PDS data dictionaries.");
        System.out.println();
        System.out.println("Required parameters:");
        System.out.println("  -config <path>      Configuration file.");
        System.out.println("Optional parameters:");
        System.out.println("  -solrUrl <url>      Solr URL. Default is http://localhost:8983/solr");
        System.out.println("  -zkHost <host>      ZooKeeper connection string, <host:port>[,<host:port>][/path]");
        System.out.println("                      For example, zk1:2181,zk2:2181,zk3:2181/solr"); 
        System.out.println("  -collection <name>  Solr collection name. Default value is 'registry'");
        System.out.println("  -log <file>         Log file. Default is /tmp/registry/registry.log.");
        System.out.println("  -v <level>          Logger verbosity: 0=Debug, 1=Info (default), 2=Warning, 3=Error.");
        System.out.println();
    }

    
    private void updateSolrSchema(Configuration cfg, SolrClient client, String collectionName) throws Exception
    {
        SolrSchemaUpdater upd = new SolrSchemaUpdater(cfg, client, collectionName);
        
        for(File file: cfg.dataDicFiles)
        {
            LOG.info("Processing data dictionary " + file.getAbsolutePath());
            JsonDDParser parser = new JsonDDParser(file);
            DataDictionary dd = parser.parse();
            parser.close();
            
            upd.updateSolrSchema(dd);
        }
    }
}
