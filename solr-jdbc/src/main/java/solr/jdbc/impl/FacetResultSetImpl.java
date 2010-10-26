package solr.jdbc.impl;

import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.FacetField.Count;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import solr.jdbc.SolrColumn;

/**
 * ResultSet for facet query
 *
 * @author kawasima
 *
 */
public class FacetResultSetImpl extends AbstractResultSet {
	public FacetResultSetImpl(QueryResponse response, ResultSetMetaDataImpl metaData) {
		this.metaData = metaData;
		docList = new SolrDocumentList();
		for (FacetField field : response.getFacetFields()) {
			try {
				int columnIndex = metaData.findColumn(new DefaultSolrColumn(field.getName()).getColumnName());
				String columnName = metaData.getSolrColumnName(columnIndex);
				for (Count count : field.getValues()) {
					SolrDocument doc = new SolrDocument();
					doc.setField(columnName, count.getName());
					for(SolrColumn countColumn : metaData.getCountColumnList()) {
						doc.setField(countColumn.getResultName(), count.getCount());
					}
					docList.add(doc);
				}
			} catch(Exception ignore) {
				// カラムが見つからない場合はスキップ
				ignore.printStackTrace();
			}
		}
	}

}
