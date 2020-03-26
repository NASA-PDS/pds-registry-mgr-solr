package gov.nasa.pds.registry.mgr.util;

import java.io.ByteArrayInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.InflaterInputStream;


public class EmbeddedBlobExporter
{
    public EmbeddedBlobExporter()
    {
    }

    
    public void export(byte[] blob, String filePath) throws Exception
    {
        ByteArrayInputStream bais = new ByteArrayInputStream(blob);
        InflaterInputStream source = new InflaterInputStream(bais);

        FileOutputStream dest = new FileOutputStream(filePath);
    
        copy(source, dest);
        dest.close();
    }


    private static void copy(InputStream source, OutputStream dest) throws Exception
    {
        byte[] buf = new byte[1024];

        int count = 0;
        while((count = source.read(buf)) >= 0)
        {
            dest.write(buf, 0, count);
        }
    }

}
