package gov.nasa.pds.registry.mgr.schema;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

public class AttributeParser
{
    private JsonReader rd;
    private int attrCount; 

    private Set<String> dataTypes;
    private Map<String, String> id2type;
    
    public AttributeParser(JsonReader rd)
    {
        this.rd = rd;
        dataTypes = new TreeSet<>();
        id2type = new HashMap<>(2000);
    }


    public Set<String> getDataTypes()
    {
        return dataTypes;
    }
    
    
    public Map<String, String> getIdToTypeMap()
    {
        return id2type;
    }
    
    
    public void parseAttr() throws Exception
    {
        String id = null;
        String dataType = null;
        
        attrCount++;
        
        rd.beginObject();
        
        while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
        {
            String name = rd.nextName();
            if("identifier".equals(name))
            {
                id = rd.nextString();
            }
            else if("dataType".equals(name))
            {
                dataType = rd.nextString();
            }
            else
            {
                rd.skipValue();
            }
        }
        
        rd.endObject();
        
        if(id == null) throw new Exception("Missing identifier in attribute definition. Index = " + attrCount);
        if(dataType == null) throw new Exception("Missing dataType in attribute definition. ID = " + id);
        
        dataTypes.add(dataType);
        id2type.put(id, dataType);
    }

}
