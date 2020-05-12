package gov.nasa.pds.registry.mgr.schema.dd;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import gov.nasa.pds.registry.mgr.util.CloseUtils;

public class Pds2SolrDataTypeMap
{
    private Logger LOG;
    private Map<String, String> map;
    
    
    public Pds2SolrDataTypeMap()
    {
        LOG = LogManager.getLogger(getClass());
        map = new HashMap<>();
    }

    
    public String getSolrType(String pdsType)
    {
        String solrType = map.get(pdsType);
        if(solrType != null) return solrType;
        
        solrType = guessType(pdsType);
        LOG.warn("No PDS to Solr data type mapping for '" 
                + pdsType + "'. Will use '" + solrType + "'");

        map.put(pdsType, solrType);
        return solrType;
    }
    
    
    private String guessType(String str)
    {
        str = str.toLowerCase();
        if(str.contains("_real")) return "pdouble";
        if(str.contains("_integer")) return "pint";
        if(str.contains("_string")) return "string";
        if(str.contains("_text")) return "text_general";
        if(str.contains("_date")) return "pdate";
        if(str.contains("_boolean")) return "boolean";        
        
        return "string";
    }
    
    
    public void load(File file) throws Exception
    {
        LOG.info("Loading data type configuration from " + file.getAbsolutePath());
        
        BufferedReader rd = null;
        
        try
        {
            rd = new BufferedReader(new FileReader(file));
        }
        catch(Exception ex)
        {
            throw new Exception("Could not open data type configuration file '" + file.getAbsolutePath());
        }
        
        try
        {
            String line;
            while((line = rd.readLine()) != null)
            {
                line = line.trim();
                if(line.startsWith("#") || line.isEmpty()) continue;
                String[] tokens = line.split("=");
                if(tokens.length != 2) 
                {
                    throw new Exception("Invalid entry in data type configuration file " 
                            + file.getAbsolutePath() + ": " + line);
                }
                
                String key = tokens[0].trim();
                if(key.isEmpty()) 
                {
                    throw new Exception("Empty key in data type configuration file " 
                            + file.getAbsolutePath() + ": " + line);
                }
                
                String value = tokens[1].trim();
                if(key.isEmpty())
                {
                    throw new Exception("Empty value in data type configuration file " 
                            + file.getAbsolutePath() + ": " + line);
                }
                
                map.put(key, value);
            }
        }
        finally
        {
            CloseUtils.close(rd);
        }
    }
    
    
    public void debug()
    {
        for(String key: map.keySet())
        {
            String val = map.get(key);
            System.out.println(key + "  -->  " + val);
        }
    }
}
