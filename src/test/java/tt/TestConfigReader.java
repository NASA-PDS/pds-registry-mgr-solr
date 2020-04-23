package tt;

import java.io.File;

import gov.nasa.pds.registry.mgr.schema.cfg.ConfigReader;
import gov.nasa.pds.registry.mgr.schema.cfg.Configuration;

public class TestConfigReader
{

    public static void main(String[] args) throws Exception
    {
        ConfigReader rd = new ConfigReader();
        Configuration conf = rd.read(new File("/tmp/schema/example.xml"));
        
        System.out.println("Data Dictionary:");
        for(File file: conf.dataDicFiles)
        {
            System.out.println("  " + file.getAbsolutePath());
        }
        
        if(conf.customClassGens != null)
        {
            System.out.println("\nCustom Generators:");
            
            for(String key: conf.customClassGens.keySet())
            {
                File file = conf.customClassGens.get(key);
                System.out.println("  " + key + "  -->  " + file);
            }
        }

        if(conf.dataTypeFiles != null)
        {
            System.out.println("\nData Type Files:");

            for(File file: conf.dataTypeFiles)
            {
                System.out.println("  " + file.getAbsolutePath());
            }
        }
    }

}
