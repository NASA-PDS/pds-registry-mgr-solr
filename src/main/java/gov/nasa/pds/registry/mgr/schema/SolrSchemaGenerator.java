package gov.nasa.pds.registry.mgr.schema;

import java.io.Writer;
import java.util.Map;
import java.util.Set;

import gov.nasa.pds.registry.mgr.schema.cfg.Configuration;
import gov.nasa.pds.registry.mgr.schema.dd.DDAttr;
import gov.nasa.pds.registry.mgr.schema.dd.DDClass;
import gov.nasa.pds.registry.mgr.schema.dd.DataDictionary;
import gov.nasa.pds.registry.mgr.schema.dd.Pds2SolrDataTypeMap;
import gov.nasa.pds.registry.mgr.util.SolrSchemaUtils;


public class SolrSchemaGenerator
{
    private Configuration cfg;
    private Pds2SolrDataTypeMap dtMap;


    public SolrSchemaGenerator(Configuration cfg)
    {
        this.cfg = cfg;
        this.dtMap = new Pds2SolrDataTypeMap();
    }


    public void generateSolrSchema(DataDictionary dd, Writer writer) throws Exception
    {
        Map<String, String> id2type = dd.getAttributeDataTypeMap();
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

            // Process attributes
            for(DDAttr attr: ddClass.attributes)
            {
                String pdsDataType = id2type.get(attr.id);
                if(pdsDataType == null) throw new Exception("No data type mapping for attribute " + attr.id);
                
                String fieldName = ddClass.nsName + "." + attr.nsName;
                String solrDataType = dtMap.getSolrType(pdsDataType);
                SolrSchemaUtils.writeSchemaField(writer, fieldName, solrDataType);
            }
        }
    }

}
