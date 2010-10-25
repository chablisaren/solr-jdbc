package solr.jdbc.command;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.insert.Insert;

import org.apache.solr.common.SolrInputDocument;

import solr.jdbc.SolrColumn;
import solr.jdbc.impl.DatabaseMetaDataImpl;
import solr.jdbc.message.DbException;
import solr.jdbc.message.ErrorCode;
import solr.jdbc.parser.ExpressionParser;
import solr.jdbc.util.SolrDocumentUtil;
import solr.jdbc.value.SolrValue;

public class InsertCommand extends Command {

	private final Insert insStmt;
	private ExpressionParser expressionParser;

	public InsertCommand(Insert stmt) {
		this.insStmt = stmt;
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
		String tableName = insStmt.getTable().getName();

		List<SolrColumn> columns = null;
		try {
			DatabaseMetaDataImpl metaData= (DatabaseMetaDataImpl)this.conn.getMetaData();
			if (insStmt.getColumns() == null) {
				columns = metaData.getSolrColumns(tableName);
			} else {
				columns = new ArrayList<SolrColumn>();
				for(Column column : (List<Column>)insStmt.getColumns()) {
					String columnName = column.getColumnName();
					columns.add(metaData.getSolrColumn(tableName, columnName));
				}
			}
		} catch (SQLException e) {
			throw DbException.get(ErrorCode.GENERAL_ERROR, e);
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
			throw DbException.get(ErrorCode.GENERAL_ERROR, e);
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
