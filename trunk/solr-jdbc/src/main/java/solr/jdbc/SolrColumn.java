package solr.jdbc;

import solr.jdbc.value.SolrType;

public class SolrColumn {
	private String tableName;
	private String columnName;
	private SolrType type;
	private SolrType originalType;

	public SolrColumn(String tableName, String columnName, SolrType type) {
		this.tableName = tableName;
		this.columnName = columnName;
		this.type = type;
	}
	
	public SolrColumn(String solrColumnName) {
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

	public SolrType getType() {
		return type;
	}

	/**
	 * Solr内部のフィールド名を返します
	 * 
	 * @return Solrの内部フィールド名
	 */
	public String getSolrColumnName() {
		String typeName = (originalType == null) ? type.name() : "M_"+originalType.name();
		return tableName + "." + columnName + "." + typeName;
	}

	/**
	 * テーブル名を返します
	 * 
	 * @return テーブル名
	 */
	public String getTableName() {
		return tableName;
	}

	/**
	 * JDBCとしてのカラム名を返します
	 * 
	 * @return JDBCカラム名
	 */
	public String getColumnName() {
		return columnName;
	}
}
