package com.google.code.solr_jdbc.impl;

import java.net.MalformedURLException;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.params.HttpClientParams;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;



public class CommonsHttpConnectionImpl extends SolrConnection {
	private int timeout = 0;
	
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

	@Override
	public int getQueryTimeout() {
		return timeout;
	}
	
	@Override
	public void setQueryTimeout(int timeout) {
		this.timeout = timeout;
		((CommonsHttpSolrServer)getSolrServer()).setConnectionTimeout(timeout*1000);
		((CommonsHttpSolrServer)getSolrServer()).setSoTimeout(timeout*1000);
	}
}
