package com.google.code.solr_jdbc.impl;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.lang.ArrayUtils;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.IndexSchema;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;



public class EmbeddedConnectionImpl extends SolrConnection {
	private CoreContainer coreContainer;
	private int timeout = 0;

	private static Properties defaults;
	static {
		defaults = new Properties();
		defaults.put("DATA_DIR", "target/solrdata");
		defaults.put("SOLR_HOME", "target/test-classes");
	}

	private Properties options = new Properties(defaults);

	public EmbeddedConnectionImpl(String serverUrl)
			throws ParserConfigurationException, IOException, SAXException {
		super(serverUrl);
		String[] tokens = serverUrl.split(";");
		if(tokens != null && tokens.length > 1) {
			tokens = (String[])ArrayUtils.remove(tokens, 0);
			for (String token : tokens) {
				String[] params = token.split("=", 2);
				options.put(params[0], params[1]);
			}
		}
		coreContainer = new CoreContainer(options.getProperty("DATA_DIR"));
		SolrConfig solrConfig = new SolrConfig(options.getProperty("DATA_DIR"),
				new InputSource(new FileInputStream(options.getProperty("SOLR_HOME")+"/solrconfig.xml")));
		IndexSchema indexSchema = new IndexSchema(solrConfig, serverUrl,
				new InputSource(new FileInputStream(options.getProperty("SOLR_HOME")+"/schema.xml")));
		CoreDescriptor coreDescriptor = new CoreDescriptor(coreContainer, "",
				solrConfig.getResourceLoader().getInstanceDir());
		SolrCore core = new SolrCore(null, options.getProperty("DATA_DIR"), solrConfig, indexSchema, coreDescriptor);
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
