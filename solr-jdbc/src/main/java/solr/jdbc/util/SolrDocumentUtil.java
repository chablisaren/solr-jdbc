package solr.jdbc.util;

import org.apache.solr.common.SolrInputDocument;

import solr.jdbc.value.SolrValue;
import solr.jdbc.value.ValueArray;

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
