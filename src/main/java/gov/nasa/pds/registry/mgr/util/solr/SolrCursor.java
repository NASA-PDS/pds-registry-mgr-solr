package gov.nasa.pds.registry.mgr.util.solr;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CursorMarkParams;


public class SolrCursor
{
    private SolrClient client;
    private String collectionName;
    private SolrQuery query;    
    
    private boolean done;
    
    private String cursorMark;
    private QueryResponse response;
    
    
    public SolrCursor(SolrClient client, String collectionName, SolrQuery query)
    {
        this.client = client;
        this.collectionName = collectionName;
        this.query = query;
        
        this.done = false;
        this.cursorMark = CursorMarkParams.CURSOR_MARK_START;
    }
    
    
    public boolean next() throws Exception
    {
        if(done) return false;

        query.set(CursorMarkParams.CURSOR_MARK_PARAM, cursorMark);
        response = client.query(collectionName, query);

        String nextCursorMark = response.getNextCursorMark();        
        if(cursorMark.equals(nextCursorMark))
        {
            done = true;
        }
        
        cursorMark = nextCursorMark;
        return true;
    }
    
    
    public SolrDocumentList getResults()
    {
        return response.getResults();
    }
}
