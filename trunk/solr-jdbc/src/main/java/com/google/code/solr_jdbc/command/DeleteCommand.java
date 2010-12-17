package com.google.code.solr_jdbc.command;

import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.UpdateResponse;

import com.google.code.solr_jdbc.impl.DatabaseMetaDataImpl;
import com.google.code.solr_jdbc.message.DbException;
import com.google.code.solr_jdbc.message.ErrorCode;
import com.google.code.solr_jdbc.parser.ConditionParser;
import com.google.code.solr_jdbc.value.SolrValue;


import net.sf.jsqlparser.statement.delete.Delete;

public class DeleteCommand extends Command {
	private final Delete delStmt;
	private ConditionParser conditionParser;

	public DeleteCommand(Delete stmt) {
		this.delStmt = stmt;
	}

	@Override
	public ResultSet executeQuery() {
		throw DbException.get(ErrorCode.METHOD_ONLY_ALLOWED_FOR_QUERY);
	}

	@Override
	public int executeUpdate() {
		List<SolrValue> queryParams = parameters.subList(0, parameters.size());
		SolrQuery query = new SolrQuery(conditionParser.getQuery(queryParams.toArray(new SolrValue[0])));
		UpdateResponse response = null;
		try {
			 response = conn.getSolrServer().deleteByQuery(query.getQuery());
		} catch (SolrServerException e) {
			throw DbException.get(ErrorCode.GENERAL_ERROR, e, "Solr Server Error");
		} catch (IOException e) {
			throw DbException.get(ErrorCode.IO_EXCEPTION, e);
		}
		// TODO 更新件数を返す
		return 0;
	}

	@Override
	public boolean isQuery() {
		return false;
	}

	@Override
	public void parse() {
		DatabaseMetaData metaData = null;

		try {
			metaData= this.conn.getMetaData();
		} catch (SQLException e) {
			throw DbException.get(ErrorCode.GENERAL_ERROR, e, "Solr Server Error");
		}
		String tableName = delStmt.getTable().getName();

		// Where句の解析
		conditionParser = new ConditionParser((DatabaseMetaDataImpl)metaData);
		conditionParser.setTableName(tableName);
		delStmt.getWhere().accept(conditionParser);
		initParameters(conditionParser.getParameterSize());
	}

}
