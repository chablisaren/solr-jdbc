package com.google.code.solr_jdbc.command;

import java.io.IOException;
import java.util.ArrayList;

import net.sf.jsqlparser.statement.delete.Delete;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;

import com.google.code.solr_jdbc.expression.Parameter;
import com.google.code.solr_jdbc.impl.AbstractResultSet;
import com.google.code.solr_jdbc.impl.DatabaseMetaDataImpl;
import com.google.code.solr_jdbc.message.DbException;
import com.google.code.solr_jdbc.message.ErrorCode;
import com.google.code.solr_jdbc.parser.ConditionParser;

public class DeleteCommand extends Command {
	private final Delete delStmt;
	private ConditionParser conditionParser;
	private String tableName;

	public DeleteCommand(Delete stmt) {
		this.parameters = new ArrayList<Parameter>();
		this.delStmt = stmt;
	}

	@Override
	public AbstractResultSet executeQuery() {
		throw DbException.get(ErrorCode.METHOD_ONLY_ALLOWED_FOR_QUERY);
	}

	@Override
	public int executeUpdate() {
		String queryString;
		long num = 0;
		if(conditionParser == null) {
			// select all records if there is no WHERE clause.
			queryString = "id:@"+delStmt.getTable().getName()+".*";
		} else {
			queryString = conditionParser.getQuery(parameters);
		}

		SolrQuery query = new SolrQuery(queryString);

		try {
			// We don't need to fetch rows.
			query.set("rows", "0");
			QueryResponse response = conn.getSolrServer().query(query);
			num = response.getResults().getNumFound();
		} catch (SolrServerException e) {
			throw DbException.get(ErrorCode.GENERAL_ERROR, e, "Solr Server Error:"+e.getMessage());
		}

		try {
			conn.getSolrServer().deleteByQuery(query.getQuery());
		} catch (SolrServerException e) {
			throw DbException.get(ErrorCode.GENERAL_ERROR, e, "Solr Server Error:"+e.getMessage());
		} catch (IOException e) {
			throw DbException.get(ErrorCode.IO_EXCEPTION, e, e.getMessage());
		}

		return (int)num;
	}

	@Override
	public boolean isQuery() {
		return false;
	}

	@Override
	public void parse() {
		DatabaseMetaDataImpl metaData= this.conn.getMetaDataImpl();

		tableName = delStmt.getTable().getName();
		if(!metaData.hasTable(tableName))
			throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND, tableName);
		tableName = metaData.getOriginalTableName(tableName);

		// Where句の解析
		if(delStmt.getWhere() != null) {
			conditionParser = new ConditionParser((DatabaseMetaDataImpl)metaData);
			conditionParser.setTableName(tableName);
			delStmt.getWhere().accept(conditionParser);
			parameters = conditionParser.getParameters();
		}
	}

}
