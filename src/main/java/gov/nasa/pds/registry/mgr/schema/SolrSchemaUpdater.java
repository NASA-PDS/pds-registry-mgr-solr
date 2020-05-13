package gov.nasa.pds.registry.mgr.schema;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;

import gov.nasa.pds.registry.mgr.schema.cfg.Configuration;
import gov.nasa.pds.registry.mgr.schema.dd.DDAttr;
import gov.nasa.pds.registry.mgr.schema.dd.DDClass;
import gov.nasa.pds.registry.mgr.schema.dd.DataDictionary;
import gov.nasa.pds.registry.mgr.schema.dd.Pds2SolrDataTypeMap;
import gov.nasa.pds.registry.mgr.util.CloseUtils;
import gov.nasa.pds.registry.mgr.util.SolrUtils;

/**
 * Updates Solr schema by calling Solr schema API  
 * @author karpenko
 */
public class SolrSchemaUpdater
{
    private Logger LOG;
    
    private Configuration cfg;
    private Pds2SolrDataTypeMap dtMap;
    private SolrClient solrClient;
    private String solrCollectionName;
    private Set<String> existingFieldNames;
    
    private List<SchemaRequest.Update> batch = new ArrayList<>();
    private int totalCount;
    private int lastBatchCount;
    private int batchSize = 100;
    
    /**
     * Constructor 
     * @param cfg Registry manager configuration
     * @param solrClient Solr client
     * @param solrCollectionName Solr collection name
     * @throws Exception
     */
    public SolrSchemaUpdater(Configuration cfg, SolrClient solrClient, String solrCollectionName) throws Exception
    {
        LOG = LogManager.getLogger(getClass());

        this.cfg = cfg;
        this.solrClient = solrClient;
        this.solrCollectionName = solrCollectionName;
        
        // Load PDS to Solr data type mapping files
        dtMap = loadDataTypeMap();
        
        // Get a list of existing field names from Solr
        this.existingFieldNames = SolrUtils.getFieldNames(solrClient, solrCollectionName);
    }


    /**
     * Load PDS to Solr data type map(s)
     * @return
     * @throws Exception
     */
    private Pds2SolrDataTypeMap loadDataTypeMap() throws Exception
    {
        Pds2SolrDataTypeMap map = new Pds2SolrDataTypeMap();
        if(cfg.dataTypeFiles != null)
        {
            for(File file: cfg.dataTypeFiles)
            {
                LOG.info("Loading PDS to Solr data type mapping from " + file.getAbsolutePath());
                map.load(file);
            }
        }
        
        return map;
    }

    
    /**
     * Add fields from data dictionary to Solr schema. Ignore existing fields.
     * @param dd
     * @throws Exception
     */
    public void updateSolrSchema(DataDictionary dd) throws Exception
    {
        lastBatchCount = 0;
        totalCount = 0;
        batch.clear();
        
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
        
        finish();
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
            addSolrField(fieldName, solrDataType);
        }
    }

    
    private void addSolrField(String name, String type) throws Exception
    {
        if(existingFieldNames.contains(name)) return;
        
        // Create add field request to the batch
        batch.add(SolrUtils.createAddFieldRequest(name, type));
        totalCount++;

        // Commit if reached batch/commit size
        if(totalCount % batchSize == 0)
        {
            LOG.info("Adding fields " + (lastBatchCount+1) + "-" + totalCount);
            SolrUtils.multiUpdate(solrClient, solrCollectionName, batch);
            lastBatchCount = totalCount;
            batch.clear();
        }
    }
    
    
    private void finish() throws Exception
    {
        if(batch.isEmpty()) return;
        
        LOG.info("Adding fields " + (lastBatchCount+1) + "-" + totalCount);
        SolrUtils.multiUpdate(solrClient, solrCollectionName, batch);
        lastBatchCount = totalCount;
        batch.clear();
    }
}
