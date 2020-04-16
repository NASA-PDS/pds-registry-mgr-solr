package tt;

import java.io.File;

import gov.nasa.pds.registry.mgr.schema.JsonSchemaParser;


public class TestJsonParser
{

    public static void main(String[] args) throws Exception
    {
        //String filePath = "/tmp/schema/PDS4_PDS_JSON_1D00.JSON";
        String filePath = "/tmp/schema/PDS4_CART_1D00_1933.JSON";
        //String filePath = "/tmp/schema/PDS4_GEOM_1B10_1700.JSON";
        
        JsonSchemaParser parser = new JsonSchemaParser(new File(filePath));
        
        parser.parse();
        
        parser.generateSolrSchema();
        
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
