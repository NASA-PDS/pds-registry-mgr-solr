package tt;

import gov.nasa.pds.registry.mgr.schema.JsonSchemaParser;


public class TestJsonParser
{

    public static void main(String[] args) throws Exception
    {
        JsonSchemaParser parser = new JsonSchemaParser("/tmp/schema/PDS4_PDS_JSON_1D00.JSON");
        parser.parse();
        
        parser.genSolrFields();
        
        parser.close();
    }

    
    private static void printDataTypes(JsonSchemaParser parser)
    {
        for(String str: parser.getDataTypes())
        {
            System.out.println(str);
        }
    }
}
