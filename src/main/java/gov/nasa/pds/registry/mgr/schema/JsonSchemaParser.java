package gov.nasa.pds.registry.mgr.schema;

import java.io.File;
import java.io.FileReader;
import java.io.Writer;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

import gov.nasa.pds.registry.mgr.util.SolrSchemaUtils;


public class JsonSchemaParser
{
    private JsonReader rd;
    private ClassParser classParser;
    private AttributeParser attrParser;
    private Pds2SolrDataTypeMap dtMap;
    
    public JsonSchemaParser(File file) throws Exception
    {
        rd = new JsonReader(new FileReader(file));
        classParser = new ClassParser(rd);
        attrParser = new AttributeParser(rd);
        
        dtMap = new Pds2SolrDataTypeMap();
    }
    
    
    public void close()
    {
        try 
        {
            rd.close();
        }
        catch(Exception ex)
        {
            // Ignore
        }
    }
    
    
    public void parse() throws Exception
    {
        rd.beginArray();
        
        while(rd.hasNext() && rd.peek() != JsonToken.END_ARRAY)
        {
            rd.beginObject();

            while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
            {
                String name = rd.nextName();
                if("dataDictionary".equals(name))
                {
                    parseDataDic();
                }
                else
                {
                    rd.skipValue();
                }
            }
            
            rd.endObject();
        }
        
        rd.endArray();
    }
    
    
    public Set<String> getDataTypes()
    {
        return attrParser.getDataTypes();
    }
    
    
    public void generateSolrSchema(Writer writer) throws Exception
    {
        Map<String, String> id2type = attrParser.getIdToTypeMap();
        
        for(DDClass ddClass: classParser.getClassMap().values())
        {
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

    
    public void printClasses() throws Exception
    {
        Map<String, String> id2type = attrParser.getIdToTypeMap();
        
        Set<String> classFilter = new HashSet<>();
        classFilter.add("pds.Table_Character");
        classFilter.add("pds.Record_Character");
        classFilter.add("pds.Field_Character");
        classFilter.add("pds.Group_Field_Character");
        
        for(DDClass ddClass: classParser.getClassMap().values())
        {
            if(!classFilter.contains(ddClass.nsName)) continue;
            
            System.out.println(ddClass.nsName);
            
            for(DDAttr attr: ddClass.attributes)
            {
                String pdsDataType = id2type.get(attr.id);
                if(pdsDataType == null) throw new Exception("No data type mapping for attribute " + attr.id);
                
                System.out.println("    " + attr.nsName + "  -->  " + pdsDataType);
            }
        }
    }
    
    
    private void parseDataDic() throws Exception
    {
        rd.beginObject();

        while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
        {
            String name = rd.nextName();
            if("classDictionary".equals(name))
            {
                parseClassDic();
            }
            else if("attributeDictionary".equals(name))
            {
                parseAttrDic();
            }
            else
            {
                rd.skipValue();
            }
        }
        
        rd.endObject();
    }

    
    private void parseClassDic() throws Exception
    {
        rd.beginArray();
        
        while(rd.hasNext() && rd.peek() != JsonToken.END_ARRAY)
        {
            rd.beginObject();

            while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
            {
                String name = rd.nextName();
                if("class".equals(name))
                {
                    classParser.parseClass();
                }
                else
                {
                    rd.skipValue();
                }
            }
            
            rd.endObject();
        }
        
        rd.endArray();
    }

    
    private void parseAttrDic() throws Exception
    {
        rd.beginArray();
        
        while(rd.hasNext() && rd.peek() != JsonToken.END_ARRAY)
        {
            rd.beginObject();

            while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
            {
                String name = rd.nextName();
                if("attribute".equals(name))
                {
                    attrParser.parseAttr();
                }
                else
                {
                    rd.skipValue();
                }
            }
            
            rd.endObject();
        }
        
        rd.endArray();
    }

}
