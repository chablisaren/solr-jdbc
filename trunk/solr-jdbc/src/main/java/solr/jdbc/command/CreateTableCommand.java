package solr.jdbc.command;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;

import solr.jdbc.ColumnSpec;
import solr.jdbc.message.DbException;
import solr.jdbc.message.ErrorCode;
import solr.jdbc.value.DataType;

public class CreateTableCommand extends Command{
	private CreateTable createTable;
	
	public CreateTableCommand(CreateTable statement) {
		this.createTable = statement;
	}

	@Override
	public void parse() {
		
	}

	@Override
	public ResultSet executeQuery() {
		throw DbException.get(ErrorCode.METHOD_ONLY_ALLOWED_FOR_QUERY);
	}

	@Override
	public int executeUpdate() {
		SolrQuery query = new SolrQuery();
		query.setQuery("meta.name:"+createTable.getTable().getName());
		try {
			QueryResponse response =  conn.getSolrServer().query(query);
			if (!response.getResults().isEmpty()) {
				throw DbException.get(ErrorCode.TABLE_OR_VIEW_ALREADY_EXISTS);
			}
		} catch (SolrServerException e) {
			throw DbException.get(ErrorCode.GENERAL_ERROR, e);
		}
		SolrInputDocument doc = new SolrInputDocument();
		doc.setField("meta.name", createTable.getTable().getName());

		for(Object elm : createTable.getColumnDefinitions()) {
			ColumnDefinition columnDef = (ColumnDefinition)elm;
			String sqlTypeName = columnDef.getColDataType().getDataType();
			ColumnSpec spec = new ColumnSpec(columnDef.getColumnSpecStrings());
			if (spec.isArray()) {
				DataType dt = DataType.getTypeByName(sqlTypeName);
				doc.addField("meta.columns", columnDef.getColumnName()+".M_"+dt.type.name());
			} else {
				DataType dt = DataType.getTypeByName(sqlTypeName);
				doc.addField("meta.columns", columnDef.getColumnName()+"."+dt.type.name());
			}
		}
		doc.setField("id", UUID.randomUUID().toString());
		try {
			conn.getSolrServer().add(doc);
			conn.commit();
		} catch (SolrServerException e) {
			throw DbException.get(ErrorCode.GENERAL_ERROR, e);
		} catch (IOException e) {
			throw DbException.get(ErrorCode.GENERAL_ERROR, e);
		} catch (SQLException e) {
			throw DbException.get(ErrorCode.GENERAL_ERROR, e);
		}
		return 0;
	}

}
