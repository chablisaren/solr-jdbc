package com.google.code.solr_jdbc;

import org.apache.commons.lang.StringUtils;

import com.google.code.solr_jdbc.value.SolrType;


public abstract class SolrColumn {
	protected String tableName;
	protected String columnName;
	protected String alias;
	protected SolrType type;
	protected SolrType originalType;

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

	public String getAlias() {
		return alias;
	}

	public void setAlias(String alias) {
		this.alias = alias;
	}

	public String getResultName() {
		if (StringUtils.isNotEmpty(alias)) {
			return alias;
		}
		return columnName;
	}
}
