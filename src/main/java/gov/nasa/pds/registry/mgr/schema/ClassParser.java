package gov.nasa.pds.registry.mgr.schema;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

public class ClassParser
{
    private JsonReader rd;
    
    public ClassParser(JsonReader rd)
    {
        this.rd = rd;
    }

    
    public void parseClass() throws Exception
    {
        String classNsName = null;
        
        rd.beginObject();
        
        while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
        {
            String name = rd.nextName();
            if("identifier".equals(name))
            {
                classNsName = stripAuthorityId(rd.nextString());
            }
            else if("associationList".equals(name))
            {
                parseAssocList(classNsName);
            }
            else
            {
                rd.skipValue();
            }
        }
        
        rd.endObject();
    }

    
    private void parseAssocList(String classNsName) throws Exception
    {
        rd.beginArray();
        
        while(rd.hasNext() && rd.peek() != JsonToken.END_ARRAY)
        {
            rd.beginObject();

            while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
            {
                String name = rd.nextName();
                if("association".equals(name))
                {
                    String attrId = parseAssoc();
                    // ID will be NULL if association type != "attribute_of"
                    if(attrId != null)
                    {
                        addAttribute(classNsName, attrId);
                    }
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

    
    private void addAttribute(String classNsName, String attrId) throws Exception
    {
        String attrNsName = extractAttrNsName(attrId);
        String fieldName = classNsName + "." + attrNsName;
        System.out.println(fieldName);
    }
    
    
    private String parseAssoc() throws Exception
    {
        String id = null;
        boolean isAttribute = false;
        
        rd.beginObject();
        
        while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
        {
            String name = rd.nextName();
            if("identifier".equals(name))
            {
                id = rd.nextString();
            }
            else if("assocType".equals(name))
            {
                String val = rd.nextString();
                if("attribute_of".equals(val))
                {
                    isAttribute = true;
                }
            }
            else
            {
                rd.skipValue();
            }
        }
        
        rd.endObject();
        
        return isAttribute ? id : null;
    }


    private static String stripAuthorityId(String str)
    {
        if(str == null) return null;
        
        int idx = str.indexOf('.');
        return (idx > 0) ? str.substring(idx + 1) : str;
    }
    
    
    private static String extractAttrNsName(String str) throws Exception
    {
        if(str == null) return null;
        
        // Remove authority ID (e.g., '0001_NASA_PDS_1')
        str = stripAuthorityId(str);
        
        // Remove class namespace and name
        int idx = str.indexOf('.');
        if(idx < 0) throw new Exception("Invalid attibute id: " + str);
        
        idx = str.indexOf('.', idx + 2);
        if(idx < 0) throw new Exception("Invalid attibute id: " + str);
        
        return str.substring(idx + 1);
    }
}


