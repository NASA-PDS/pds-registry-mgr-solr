package gov.nasa.pds.registry.mgr.schema;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.Writer;
import java.util.Map;
import java.util.Set;

import gov.nasa.pds.registry.mgr.schema.cfg.Configuration;
import gov.nasa.pds.registry.mgr.schema.dd.DDAttr;
import gov.nasa.pds.registry.mgr.schema.dd.DDClass;
import gov.nasa.pds.registry.mgr.schema.dd.DataDictionary;
import gov.nasa.pds.registry.mgr.schema.dd.Pds2SolrDataTypeMap;
import gov.nasa.pds.registry.mgr.util.CloseUtils;
import gov.nasa.pds.registry.mgr.util.SolrSchemaUtils;


public class SolrSchemaGenerator
{
    private Configuration cfg;
    private Writer writer;
    
    private Pds2SolrDataTypeMap dtMap;
    

    public SolrSchemaGenerator(Configuration cfg, Writer writer) throws Exception
    {
        if(cfg == null) throw new IllegalArgumentException("Missing configuration parameter.");
        if(writer == null) throw new IllegalArgumentException("Missing writer parameter.");
        
        this.cfg = cfg;
        this.writer = writer;
        
        dtMap = new Pds2SolrDataTypeMap();
        if(cfg.dataTypeFiles != null)
        {
            for(File file: cfg.dataTypeFiles)
            {
                dtMap.load(file);
            }
        }
    }


    public void generateSolrSchema(DataDictionary dd) throws Exception
    {
        Map<String, String> attrId2Type = dd.getAttributeDataTypeMap();
        Set<String> dataTypes = dd.getDataTypes();
        
        for(DDClass ddClass: dd.getClassMap().values())
        {
            // Skip type definitions.
            if(dataTypes.contains(ddClass.nsName)) continue;
            
            // Apply class filters
            if(cfg.includeClasses != null && cfg.includeClasses.size() > 0)
            {
                if(!cfg.includeClasses.contains(ddClass.nsName)) continue;
            }
            if(cfg.excludeClasses != null && cfg.excludeClasses.size() > 0)
            {
                if(cfg.excludeClasses.contains(ddClass.nsName)) continue;
            }

            File customFile = (cfg.customClassGens == null) ? null : cfg.customClassGens.get(ddClass.nsName);
            if(customFile != null)
            {
                addCustomFields(ddClass, customFile);
            }
            else
            {
                addClassAttributes(ddClass, attrId2Type);
            }
        }
    }
    
    
    private void addCustomFields(DDClass ddClass, File file) throws Exception
    {
        BufferedReader rd = null;
        
        try
        {
            rd = new BufferedReader(new FileReader(file));
        }
        catch(Exception ex)
        {
            throw new Exception("Could not open custom generator for class '" 
                    + ddClass.nsName + "':  " + file.getAbsolutePath());
        }
        
        try
        {
            String line;
            while((line = rd.readLine()) != null)
            {
                writer.write(line);
                writer.write("\n");
            }
        }
        finally
        {
            CloseUtils.close(rd);
        }
    }
    
    
    private void addClassAttributes(DDClass ddClass, Map<String, String> attrId2Type) throws Exception
    {
        for(DDAttr attr: ddClass.attributes)
        {
            String pdsDataType = attrId2Type.get(attr.id);
            if(pdsDataType == null) throw new Exception("No data type mapping for attribute " + attr.id);
            
            String fieldName = ddClass.nsName + "." + attr.nsName;
            String solrDataType = dtMap.getSolrType(pdsDataType);
            SolrSchemaUtils.writeSchemaField(writer, fieldName, solrDataType);
        }
    }

}
