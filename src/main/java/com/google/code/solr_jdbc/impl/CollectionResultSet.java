package com.google.code.solr_jdbc.impl;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import com.google.code.solr_jdbc.expression.ColumnExpression;
import com.google.code.solr_jdbc.expression.Expression;
import com.google.code.solr_jdbc.message.DbException;
import com.google.code.solr_jdbc.message.ErrorCode;
import com.google.code.solr_jdbc.value.SolrType;

public class CollectionResultSet extends AbstractResultSet {
	private List<String> columns;

	public CollectionResultSet() {
		this.docList = new SolrDocumentList();
		this.columns = new ArrayList<String>();
	}

	protected void add(List<Object> record) {
		SolrDocument doc = new SolrDocument();
		for (int i=0; i<columns.size(); i++) {
			doc.setField(columns.get(i), record.get(i));
		}
		docList.add(doc);
	}

	protected void setColumns(List<String> columnNames) {
		this.columns = columnNames;
		List<Expression> expressions = new ArrayList<Expression>();
		for(String columnName : columns) {
			ColumnExpression expression = new ColumnExpression(null, columnName, SolrType.UNKNOWN);
			expressions.add(expression);
		}
		this.metaData = new ResultSetMetaDataImpl(this, expressions, null);
	}
	@Override
	public int findColumn(String columnLabel) throws SQLException {
		int i=0;
		for(; i<columns.size(); i++) {
			if (StringUtils.equals(columnLabel, columns.get(i))) {
				break;
			}
		}
		if(i==columns.size()) {
			throw DbException.get(ErrorCode.COLUMN_NOT_FOUND, columnLabel);
		}
		return i;
	}

}
