package com.google.code.solr_jdbc.impl;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import com.google.code.solr_jdbc.SolrColumn;

public class DefaultResultSetImpl extends AbstractResultSet {
	public DefaultResultSetImpl(SolrDocumentList solrResult, ResultSetMetaDataImpl rsMetaData) {
		this.docList = solrResult;
		this.metaData = rsMetaData;

		if (metaData.getCountColumnList().size() > 0) {
			// Countがあれば0件でもResultSetが帰るため
			if(docList.size() == 0) {
				docList.add(new SolrDocument()); 
			}
			for(SolrDocument doc : docList) {
				for(SolrColumn countColumn :metaData.getCountColumnList()) {
					doc.setField(countColumn.getResultName(), solrResult.getNumFound());
				}
			}
		}
	}

}
