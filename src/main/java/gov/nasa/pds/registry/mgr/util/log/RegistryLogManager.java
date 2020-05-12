package gov.nasa.pds.registry.mgr.util.log;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RegistryLogManager
{
    public static Logger getMinInfoLogger()
    {
        return LogManager.getLogger("registry-min-info");
    }
}
