package tt;

import java.io.File;
import java.io.FileWriter;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import gov.nasa.pds.registry.mgr.schema.SolrSchemaGenerator;
import gov.nasa.pds.registry.mgr.schema.dd.DDAttr;
import gov.nasa.pds.registry.mgr.schema.dd.DDClass;
import gov.nasa.pds.registry.mgr.schema.dd.DataDictionary;
import gov.nasa.pds.registry.mgr.schema.dd.JsonDDParser;


public class TestJsonParser
{

    public static void main(String[] args) throws Exception
    {
        String filePath = "/tmp/schema/PDS4_PDS_JSON_1D00.JSON";
        //String filePath = "/tmp/schema/PDS4_CART_1D00_1933.JSON";
        //String filePath = "/tmp/schema/PDS4_GEOM_1B10_1700.JSON";
        
        JsonDDParser parser = new JsonDDParser(new File(filePath));
        DataDictionary dd = parser.parse();
        parser.close();
        

        SolrSchemaGenerator gen = new SolrSchemaGenerator();

        FileWriter writer = new FileWriter("/tmp/t1.xml");
        gen.generateSolrSchema(dd, writer);
        writer.close();
        
    }

    
    private static void printDataTypes(DataDictionary dd)
    {
        for(String str: dd.getDataTypes())
        {
            System.out.println(str);
        }
    }

    
    public void printClasses(DataDictionary dd) throws Exception
    {
        Map<String, String> id2type = dd.getAttributeDataTypeMap();
        
        Set<String> classFilter = new HashSet<>();
        
        /*
        classFilter.add("pds.Table_Character");
        classFilter.add("pds.Record_Character");
        classFilter.add("pds.Field_Character");
        classFilter.add("pds.Group_Field_Character");
        */
        
        for(DDClass ddClass: dd.getClassMap().values())
        {
            //if(!classFilter.contains(ddClass.nsName)) continue;
            
            System.out.println(ddClass.nsName);
            
            for(DDAttr attr: ddClass.attributes)
            {
                String pdsDataType = id2type.get(attr.id);
                if(pdsDataType == null) throw new Exception("No data type mapping for attribute " + attr.id);
                
                System.out.println("    " + attr.nsName + "  -->  " + pdsDataType);
            }
        }
    }
    
}
