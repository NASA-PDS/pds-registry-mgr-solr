package gov.nasa.pds.registry.mgr.schema.cfg;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.xml.xpath.XPathFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import gov.nasa.pds.registry.mgr.util.XPathUtils;
import gov.nasa.pds.registry.mgr.util.XmlDomUtils;


public class ConfigReader
{
    private Logger LOG;
    
    XPathFactory xpf = XPathFactory.newInstance();
    
    public ConfigReader()
    {
        LOG = LogManager.getLogger(getClass());
    }
    
    
    public Configuration read(File file) throws Exception
    {
        Document doc = XmlDomUtils.readXml(file);
        String rootElement = doc.getDocumentElement().getNodeName();
        if(!"schemaGen".equals(rootElement))
        {
            throw new Exception("Invalid root element '" + rootElement + "'. Expecting 'schemaGen'.");
        }
        
        Configuration cfg = new Configuration();
        cfg.ddFiles = parseDataDictionary(doc);

        return cfg;
    }


    private List<File> parseDataDictionary(Document doc) throws Exception
    {
        XPathUtils xpu = new XPathUtils();
        
        int count = xpu.getNodeCount(doc, "/schemaGen/dataDictionary");
        if(count == 0) throw new Exception("Missing required element '/schemaGen/dataDictionary'.");
        if(count > 1) throw new Exception("Could not have more than one '/schemaGen/dataDictionary' element.");
        
        List<String> files = xpu.getStringList(doc, "/schemaGen/dataDictionary/file");
        if(files == null || files.isEmpty()) 
        {
            throw new Exception("Please provide at least one data dictionary file ('/schemaGen/dataDictionary/file')."); 
        }

        Node rootNode = xpu.getFirstNode(doc, "/schemaGen/dataDictionary");
        String baseDirStr = XmlDomUtils.getAttribute(rootNode, "baseDir");
        
        File baseDir = (baseDirStr != null && !baseDirStr.isEmpty()) ? new File(baseDirStr) : null;
        
        List<File> ddFiles = new ArrayList<>();
        for(String str: files)
        {
            File file = (baseDir == null) ? new File(str) : new File(baseDir, str);
            ddFiles.add(file);
        }
        
        return ddFiles;
    }
}
