package gov.nasa.pds.registry.mgr.cmd;

import java.io.File;
import java.io.FileWriter;

import org.apache.commons.cli.CommandLine;

import gov.nasa.pds.registry.mgr.schema.SolrSchemaGenerator;
import gov.nasa.pds.registry.mgr.schema.cfg.ConfigReader;
import gov.nasa.pds.registry.mgr.schema.cfg.Configuration;
import gov.nasa.pds.registry.mgr.schema.dd.DataDictionary;
import gov.nasa.pds.registry.mgr.schema.dd.JsonDDParser;


public class GenerateSolrSchemaCmd implements CliCommand
{
    public GenerateSolrSchemaCmd()
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

        // Get a list of JSON data dictionary files to parse
        String cfgPath = cmdLine.getOptionValue("config");
        if(cfgPath == null) 
        {
            System.out.println("ERROR: Missing required parameter '-config'");
            System.out.println();
            printHelp();
            return;
        }
        
        ConfigReader cfgReader = new ConfigReader();
        Configuration cfg = cfgReader.read(new File(cfgPath));
        
        // Get output folder
        File outDir = new File(cmdLine.getOptionValue("outDir", "/tmp"));
        outDir.mkdirs();
        
        // Generate Solr schema
        generateSolrSchema(cfg, outDir);
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
        System.out.println("  -outDir <dir>   Output directory for Solr schema file. Default value is '/tmp'."); 
        System.out.println();
    }

    
    private void generateSolrSchema(Configuration cfg, File outDir) throws Exception
    {
        File outFile = new File(outDir, "solr-fields.xml");
        FileWriter writer = new FileWriter(outFile);
        
        SolrSchemaGenerator gen = new SolrSchemaGenerator(cfg, writer);
        
        for(File file: cfg.ddFiles)
        {
            JsonDDParser parser = new JsonDDParser(file);
            DataDictionary dd = parser.parse();
            parser.close();
            
            gen.generateSolrSchema(dd);
        }
        
        writer.close();
    }
}
