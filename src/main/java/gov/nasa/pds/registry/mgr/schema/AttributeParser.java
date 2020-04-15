package gov.nasa.pds.registry.mgr.schema;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

public class AttributeParser
{
    private JsonReader rd;

    public AttributeParser(JsonReader rd)
    {
        this.rd = rd;
    }
    

    public void parseAttr() throws Exception
    {
        rd.beginObject();
        
        while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
        {
            String name = rd.nextName();
            if("identifier".equals(name))
            {
                String id = rd.nextString();
                System.out.println(id);
            }
            else
            {
                rd.skipValue();
            }
        }
        
        rd.endObject();
    }

}
