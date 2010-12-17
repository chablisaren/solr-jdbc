package com.google.code.solr_jdbc.command;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.insert.Insert;

import org.apache.solr.common.SolrInputDocument;

import com.google.code.solr_jdbc.SolrColumn;
import com.google.code.solr_jdbc.impl.DatabaseMetaDataImpl;
import com.google.code.solr_jdbc.message.DbException;
import com.google.code.solr_jdbc.message.ErrorCode;
import com.google.code.solr_jdbc.parser.ExpressionParser;
import com.google.code.solr_jdbc.util.SolrDocumentUtil;
import com.google.code.solr_jdbc.value.SolrValue;


public class InsertCommand extends Command {

	private final Insert insStmt;
	private ExpressionParser expressionParser;

	public InsertCommand(Insert stmt) {
		this.insStmt = stmt;
	}

	@Override
	public boolean isQuery() {
		return false;
	}
	
	@Override
	public void parse() {
		expressionParser = new ExpressionParser();
		insStmt.getItemsList().accept(expressionParser);
		initParameters(expressionParser.getParameterSize());
	}

	@Override
	public ResultSet executeQuery() {
		throw DbException.get(ErrorCode.METHOD_ONLY_ALLOWED_FOR_QUERY);
	}

	@Override
	public int executeUpdate() {
		DatabaseMetaDataImpl metaData = this.conn.getMetaDataImpl();
		String tableName = insStmt.getTable().getName();
		List<SolrColumn> columns = metaData.getSolrColumns(tableName);
		if(columns == null)
			throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND, tableName);
		
		if (insStmt.getColumns() != null) {
			columns = new ArrayList<SolrColumn>();
			for(Column column : (List<Column>)insStmt.getColumns()) {
				SolrColumn solrColumn = metaData.getSolrColumn(tableName, column.getColumnName());
				if(solrColumn == null)
					throw DbException.get(ErrorCode.COLUMN_NOT_FOUND, column.getColumnName());
				columns.add(solrColumn);
			}
		}

		List<SolrValue> insParams = expressionParser.getParameters();

		if (columns.size() != insParams.size()) {
			throw DbException.get(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH);
		}

		bind(insParams);

		SolrInputDocument doc = new SolrInputDocument();
		for(int i=0; i<columns.size(); i++) {
			SolrDocumentUtil.setValue(doc, columns.get(i).getSolrColumnName(), insParams.get(i));
		}
		doc.setField("id", "@"+tableName+"."+UUID.randomUUID().toString());
		try {
			conn.getSolrServer().add(doc);
		} catch (Exception e) {
			throw DbException.get(ErrorCode.GENERAL_ERROR, e, "Solr Server Error");
		}

		return doc.size();
	}

	private void bind(List<SolrValue> params) {
		int bindParamsIndex = 0;
		List<SolrValue> bindParams = getParameters();
		for(int i=0; i<params.size(); i++) {
			if(params.get(i) == null) {
				params.set(i, bindParams.get(bindParamsIndex++));
			}
		}
	}

}
