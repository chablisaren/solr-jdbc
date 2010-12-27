package com.google.code.solr_jdbc.command;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.insert.Insert;

import org.apache.solr.common.SolrInputDocument;

import com.google.code.solr_jdbc.SolrColumn;
import com.google.code.solr_jdbc.expression.Item;
import com.google.code.solr_jdbc.impl.DatabaseMetaDataImpl;
import com.google.code.solr_jdbc.message.DbException;
import com.google.code.solr_jdbc.message.ErrorCode;
import com.google.code.solr_jdbc.parser.ItemListParser;
import com.google.code.solr_jdbc.util.SolrDocumentUtil;

public class InsertCommand extends Command {

	private final Insert insStmt;
	private List<Item> itemList;

	public InsertCommand(Insert stmt) {
		this.insStmt = stmt;
	}

	@Override
	public boolean isQuery() {
		return false;
	}

	@Override
	public void parse() {
		ItemListParser itemListParser = new ItemListParser();
		insStmt.getItemsList().accept(itemListParser);
		parameters = itemListParser.getParameters();
		itemList = itemListParser.getItemList();
	}

	@Override
	public ResultSet executeQuery() {
		throw DbException.get(ErrorCode.METHOD_ONLY_ALLOWED_FOR_QUERY);
	}

	@SuppressWarnings("unchecked")
	@Override
	public int executeUpdate() {
		DatabaseMetaDataImpl metaData = this.conn.getMetaDataImpl();
		String tableName = insStmt.getTable().getName();
		List<SolrColumn> columns = metaData.getSolrColumns(tableName);
		if (columns == null)
			throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND, tableName);

		if (insStmt.getColumns() != null) {
			columns = new ArrayList<SolrColumn>();
			for (Column column : (List<Column>) insStmt.getColumns()) {
				SolrColumn solrColumn = metaData.getSolrColumn(tableName,
						column.getColumnName());
				if (solrColumn == null)
					throw DbException.get(ErrorCode.COLUMN_NOT_FOUND, column
							.getColumnName());
				columns.add(solrColumn);
			}
		}

		if (columns.size() != parameters.size()) {
			throw DbException.get(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH);
		}

		SolrInputDocument doc = new SolrInputDocument();
		for (int i = 0; i < columns.size(); i++) {
			Item item = itemList.get(i);
			SolrDocumentUtil.setValue(doc, columns.get(i).getSolrColumnName(),
					item.getValue());
		}
		doc
				.setField("id", "@" + tableName + "."
						+ UUID.randomUUID().toString());
		try {
			conn.getSolrServer().add(doc);
		} catch (Exception e) {
			throw DbException.get(ErrorCode.GENERAL_ERROR, e,
					"Solr Server Error");
		}

		return doc.size();
	}
}
