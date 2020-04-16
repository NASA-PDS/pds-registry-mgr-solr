package gov.nasa.pds.registry.mgr.schema;

import java.io.File;
import java.io.FileReader;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

public class JsonSchemaParser
{
    private JsonReader rd;
    private ClassParser classParser;
    private AttributeParser attrParser;
    
    
    public JsonSchemaParser(File file) throws Exception
    {
        rd = new JsonReader(new FileReader(file));
        classParser = new ClassParser(rd);
        attrParser = new AttributeParser(rd);
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
    
    
    public void generateSolrSchema() throws Exception
    {
        Map<String, String> id2type = attrParser.getIdToTypeMap();
        
        for(ClassParser.Field field: classParser.getFields())
        {
            String pdsDataType = id2type.get(field.attrId);
            if(pdsDataType == null) throw new Exception("No data type mapping for attribute " + field.attrId);
            
            System.out.println(field.name + "  -->  " + pdsDataType);
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
