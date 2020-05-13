package gov.nasa.pds.registry.mgr.cmd;

import java.io.File;
import java.io.FileWriter;

import org.apache.commons.cli.CommandLine;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import gov.nasa.pds.registry.mgr.schema.SolrSchemaGenerator;
import gov.nasa.pds.registry.mgr.schema.cfg.ConfigReader;
import gov.nasa.pds.registry.mgr.schema.cfg.Configuration;
import gov.nasa.pds.registry.mgr.schema.dd.DataDictionary;
import gov.nasa.pds.registry.mgr.schema.dd.JsonDDParser;
import gov.nasa.pds.registry.mgr.util.log.RegistryLogManager;


public class GenerateSolrSchemaCmd implements CliCommand
{
    private Logger LOG;
    
    public GenerateSolrSchemaCmd()
    {
        // NOTE: Do not get logger here. It is not initialized yet!
    }
    
    
    @Override
    public void run(CommandLine cmdLine) throws Exception
    {
        if(cmdLine.hasOption("help"))
        {
            printHelp();
            return;
        }

        // Get a list of JSON data dictionary files to parse
        String cfgPath = cmdLine.getOptionValue("config");
        if(cfgPath == null) 
        {
            System.out.println("ERROR: Missing required parameter '-config'");
            System.out.println();
            printHelp();
            return;
        }
        
        // NOTE: Do not get logger in constructor or static initializer!
        LOG = LogManager.getLogger(getClass());
        // This logger does not depend on -v (verbosity) command line parameter.
        Logger minLogger = RegistryLogManager.getMinInfoLogger();
        
        // Read configuration file
        File cfgFile = new File(cfgPath);
        minLogger.info("Reading configuration from " + cfgFile.getAbsolutePath());
        ConfigReader cfgReader = new ConfigReader();
        Configuration cfg = cfgReader.read(cfgFile);
        
        // Get output folder
        File outDir = new File(cmdLine.getOptionValue("outDir", "/tmp/registry"));
        outDir.mkdirs();
        
        // Generate Solr schema
        generateSolrSchema(cfg, outDir);
        
        minLogger.info("Done.");
    }

    
    public void printHelp()
    {
        System.out.println("Usage: registry-manager generate-solr-schema <options>");

        System.out.println();
        System.out.println("Generate Solr schema from one or more PDS data dictionaries.");
        System.out.println();
        System.out.println("Required parameters:");
        System.out.println("  -config <path>  Configuration file.");
        System.out.println("Optional parameters:");
        System.out.println("  -outDir <dir>   Output directory for Solr schema file. Default value is '/tmp/registry'.");
        System.out.println("  -log <file>     Log file. Default is /tmp/registry/registry.log.");
        System.out.println("  -v <level>      Logger verbosity: 0=Debug, 1=Info (default), 2=Warning, 3=Error.");
        System.out.println();
    }

    
    private void generateSolrSchema(Configuration cfg, File outDir) throws Exception
    {
        File outFile = new File(outDir, "solr-fields.xml");
        LOG.info("Writing Solr fields to " + outFile.getAbsolutePath());
        
        FileWriter writer = new FileWriter(outFile);
        writer.write("<fields>\n");
        
        SolrSchemaGenerator gen = new SolrSchemaGenerator(cfg, writer);
        
        for(File file: cfg.dataDicFiles)
        {
            LOG.info("Processing data dictionary " + file.getAbsolutePath());
            JsonDDParser parser = new JsonDDParser(file);
            DataDictionary dd = parser.parse();
            parser.close();
            
            gen.generateSolrSchema(dd);
        }
        
        writer.write("</fields>\n");
        writer.close();
    }
}
