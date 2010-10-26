package solr.jdbc.impl;

import solr.jdbc.SolrColumn;
import solr.jdbc.value.SolrType;

public class DefaultSolrColumn extends SolrColumn {
	public DefaultSolrColumn(String tableName, String columnName, SolrType type) {
		this.tableName = tableName;
		this.columnName = columnName;
		this.type = type;
	}

	public DefaultSolrColumn(String solrColumnName) {
		String[] columnNameTokens = solrColumnName.split("\\.", 3);
		if (columnNameTokens.length != 3) {
			throw new IllegalArgumentException("invalid solr column name: " + solrColumnName);
		}
		this.tableName = columnNameTokens[0];
		this.columnName = columnNameTokens[1];
		if (columnNameTokens[2].startsWith("M_")) {
			type = SolrType.ARRAY;
			originalType = SolrType.valueOf(columnNameTokens[2].substring(2));
		} else {
			this.type = SolrType.valueOf(columnNameTokens[2]);
		}
	}
}
