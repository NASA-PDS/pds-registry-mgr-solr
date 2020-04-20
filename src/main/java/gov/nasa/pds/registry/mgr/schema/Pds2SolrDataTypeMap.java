package gov.nasa.pds.registry.mgr.schema;

import java.util.HashMap;
import java.util.Map;

public class Pds2SolrDataTypeMap
{
    private Map<String, String> map;
    
    
    public Pds2SolrDataTypeMap()
    {
        map = new HashMap<>();
    }

    
    public String getSolrType(String pdsType)
    {
        String solrType = map.get(pdsType);
        if(solrType != null) return solrType;
        
        solrType = guessType(pdsType);
        System.out.println("WARNING: No PDS to Solr data type mapping for '" 
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
}
