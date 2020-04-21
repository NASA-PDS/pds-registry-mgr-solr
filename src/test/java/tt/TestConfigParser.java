package tt;

import java.io.File;

import gov.nasa.pds.registry.mgr.schema.cfg.ConfigReader;
import gov.nasa.pds.registry.mgr.schema.cfg.Configuration;

public class TestConfigParser
{

    public static void main(String[] args) throws Exception
    {
        ConfigReader rd = new ConfigReader();
        Configuration conf = rd.read(new File("/tmp/schema/template.xml"));
        
        for(File file: conf.ddFiles)
        {
            System.out.println(file);
        }
    }

}
