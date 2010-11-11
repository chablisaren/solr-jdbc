package com.google.code.solr_jdbc.util;

import org.apache.solr.common.SolrInputDocument;

import com.google.code.solr_jdbc.value.SolrValue;
import com.google.code.solr_jdbc.value.ValueArray;


public class SolrDocumentUtil {
	public static void setValue(SolrInputDocument document, String columnName, SolrValue value) {
		if (value instanceof ValueArray) {
			SolrValue[] values = ((ValueArray)value).getList();
			for(SolrValue v : values) {
				document.addField(columnName, v.getString());
			}
		}
		else {
			document.setField(columnName, value.getString());
		}

	}
}
