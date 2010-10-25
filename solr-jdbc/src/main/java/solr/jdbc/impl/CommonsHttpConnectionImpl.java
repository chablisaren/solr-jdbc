package solr.jdbc.impl;

import java.net.MalformedURLException;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.params.HttpClientParams;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;



public class CommonsHttpConnectionImpl extends SolrConnection {
	public CommonsHttpConnectionImpl(String serverUrl) throws MalformedURLException {
		super(serverUrl);
		HttpClient httpClient = new HttpClient();
		HttpClientParams params = new HttpClientParams();
		httpClient.setParams(params);
		SolrServer solrServer = new CommonsHttpSolrServer(serverUrl, httpClient);
		setSolrServer(solrServer);
	}
	
	@Override
	public void close() {
		
	}

}
