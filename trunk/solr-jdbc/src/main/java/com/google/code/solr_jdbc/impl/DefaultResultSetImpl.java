package com.google.code.solr_jdbc.impl;

import org.apache.solr.common.SolrDocumentList;

public class DefaultResultSetImpl extends AbstractResultSet {
	public DefaultResultSetImpl(SolrDocumentList solrResult, ResultSetMetaDataImpl rsMetaData) {
		this.docList = solrResult;

		this.metaData = rsMetaData;
	}

}
