package com.google.code.solr_jdbc.impl;

import java.io.FileInputStream;
import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.IndexSchema;
import org.xml.sax.SAXException;



public class EmbeddedConnectionImpl extends SolrConnection {
	private CoreContainer coreContainer;
	private int timeout = 0;
	
	public EmbeddedConnectionImpl(String serverUrl)
			throws ParserConfigurationException, IOException, SAXException {
		super(serverUrl);
		coreContainer = new CoreContainer("target/solrdata");
		SolrConfig solrConfig = new SolrConfig("target/solrdata", new FileInputStream("target/test-classes/solrconfig.xml"));
		IndexSchema indexSchema = new IndexSchema(solrConfig, serverUrl, new FileInputStream("target/test-classes/schema.xml"));
		CoreDescriptor coreDescriptor = new CoreDescriptor(coreContainer, "", 
				solrConfig.getResourceLoader().getInstanceDir());
		SolrCore core = new SolrCore(null, "target/solrData", solrConfig, indexSchema, coreDescriptor);
		coreContainer.register("", core, false);
		EmbeddedSolrServer solrServer = new EmbeddedSolrServer(coreContainer, "");
		setSolrServer(solrServer);
	}
	
	@Override
	public void close() {
		coreContainer.shutdown();
	}

	@Override
	public int getQueryTimeout() {
		return timeout;
	}
	
	@Override
	public void setQueryTimeout(int timeout) {
		this.timeout = timeout;
	}
}
