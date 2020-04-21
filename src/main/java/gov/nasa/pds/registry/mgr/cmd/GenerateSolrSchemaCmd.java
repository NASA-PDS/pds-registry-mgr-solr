package gov.nasa.pds.registry.mgr.cmd;

import java.io.File;
import java.io.FileFilter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.CommandLine;

import gov.nasa.pds.registry.mgr.schema.SolrSchemaGenerator;
import gov.nasa.pds.registry.mgr.schema.dd.DataDictionary;
import gov.nasa.pds.registry.mgr.schema.dd.JsonDDParser;


public class GenerateSolrSchemaCmd implements CliCommand
{
//////////////////////////////////////////////////////////////////
    
    private static class JsonFileFilter implements FileFilter
    {
        public JsonFileFilter()
        {            
        }
        
        @Override
        public boolean accept(File file)
        {
            if(!file.isFile()) return false;
            
            String fileName = file.getName().toLowerCase();
            return fileName.endsWith(".json");
        }
    }
    
//////////////////////////////////////////////////////////////////
    
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
        String filePath = cmdLine.getOptionValue("filePath");
        if(filePath == null) 
        {
            System.out.println("ERROR: Missing required parameter '-filePath'");
            System.out.println();
            printHelp();
            return;
        }
        
        List<File> files = getFiles(filePath);
        if(files == null || files.isEmpty()) return;

        // Get output folder
        File outDir = new File(cmdLine.getOptionValue("outDir", "/tmp"));
        outDir.mkdirs();
        
        // Process JSON files
        processJsonFiles(files, outDir);
    }

    
    public void printHelp()
    {
        System.out.println("Usage: registry-manager generate-solr-schema <options>");

        System.out.println();
        System.out.println("Generate Solr schema from one or more PDS data dictionaries.");
        System.out.println();
        System.out.println("Required parameters:");
        System.out.println("  -filePath <path>  A JSON data dictionary file or a folder with JSON data dictionary files.");
        System.out.println("Optional parameters:");        
        System.out.println("  -outDir <dir>     Output directory for Solr schema file. Default value is '/tmp'."); 
        System.out.println();
    }

    
    private List<File> getFiles(String filePath) throws Exception
    {
        File file = new File(filePath);

        if(file.isDirectory())
        {
            File[] ff = file.listFiles(new JsonFileFilter());
            if(ff == null || ff.length == 0)
            {
                System.out.println("Could not find any JSON files in " + file.getAbsolutePath());
                return null;
            }
            
            return Arrays.asList(ff);
        }
        else
        {
            // Check if the file exists and has JSON extension
            if(!file.exists()) 
            {
                throw new Exception("File does not exist: " + file.getAbsolutePath());
            }
            
            if(!filePath.toLowerCase().endsWith(".json")) 
            {
                throw new Exception("Unsupported file type: " + file.getAbsolutePath());
            }

            List<File> list = new ArrayList<>(1);
            list.add(file);
            return list;
        }
    }

    
    private void processJsonFiles(List<File> files, File outDir) throws Exception
    {
        File outFile = new File(outDir, "solr-fields.xml");
        FileWriter writer = new FileWriter(outFile);
        
        SolrSchemaGenerator gen = new SolrSchemaGenerator();
        
        for(File file: files)
        {
            JsonDDParser parser = new JsonDDParser(file);
            DataDictionary dd = parser.parse();
            parser.close();
            
            gen.generateSolrSchema(dd, writer);
        }
        
        writer.close();
    }
}
