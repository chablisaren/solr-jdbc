package solr.jdbc.command;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.update.Update;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

import solr.jdbc.SolrColumn;
import solr.jdbc.impl.DatabaseMetaDataImpl;
import solr.jdbc.message.DbException;
import solr.jdbc.message.ErrorCode;
import solr.jdbc.parser.ConditionParser;
import solr.jdbc.parser.ExpressionParser;
import solr.jdbc.util.SolrDocumentUtil;
import solr.jdbc.value.SolrValue;

public class UpdateCommand extends Command {
	private final Update updStmt;
	private ExpressionParser expressionParser;
	private ConditionParser conditionParser;

	private String queryString;
	/** UPDATE文でSET句で指定されているカラム */
	private final Map<String, Integer> solrColumnNames;

	public UpdateCommand(Update stmt) {
		this.updStmt = stmt;
		solrColumnNames = new HashMap<String, Integer>();
	}

	@Override
	public boolean isQuery() {
		return false;
	}
	
	@Override
	public ResultSet executeQuery() {
		throw DbException.get(ErrorCode.METHOD_ONLY_ALLOWED_FOR_QUERY);
	}

	/**
	 * SolrにはUpdateがないので、delete and insertする。
	 */
	@Override
	public int executeUpdate() {
		List<SolrValue> queryParams = parameters.subList(expressionParser.getParameterSize(), parameters.size());
		SolrQuery query = new SolrQuery(conditionParser.getQuery(queryParams.toArray(new SolrValue[0])));
		QueryResponse response = null;
		try {
			response = conn.getSolrServer().query(query);
		} catch (SolrServerException e) {
			throw DbException.get(ErrorCode.GENERAL_ERROR, e.getMessage());
		}
		// 対象件数が0件の場合は更新を行わずに0を返す
		if (response.getResults().getNumFound() == 0) {
			return 0;
		}

		List<SolrValue> updParams = expressionParser.getParameters();
		bind(updParams);

		SolrDocumentList docList = response.getResults();
		List<SolrInputDocument> inDocs = new ArrayList<SolrInputDocument>();
		Iterator<SolrDocument> iter = docList.iterator();
		while(iter.hasNext()) {
			SolrDocument doc = iter.next();
			SolrInputDocument inDoc = new SolrInputDocument();

			int columnIndex = 0;
			for(String fieldName : doc.getFieldNames()) {
				// UPDATEのSET句に含まれるカラムは、その値をセットする
				if (solrColumnNames.containsKey(fieldName)) {
					SolrDocumentUtil.setValue(inDoc, fieldName, updParams.get(columnIndex++));
				}
				// そうでない場合は、SELECTしてきた値をそのままセットする
				else {
					inDoc.setField(fieldName, doc.getFieldValue(fieldName));
				}
			}
			inDocs.add(inDoc);
		}

		try {
			conn.getSolrServer().deleteByQuery(query.getQuery());
			conn.getSolrServer().add(inDocs);
		} catch (Exception e) {
			throw DbException.get(ErrorCode.GENERAL_ERROR, e.getMessage());
		}

		return inDocs.size();
	}

	@Override
	public void parse() {
		DatabaseMetaData metaData = null;

		try {
			metaData= this.conn.getMetaData();
		} catch (SQLException e) {
			throw DbException.get(ErrorCode.GENERAL_ERROR, e);
		}
		String tableName = updStmt.getTable().getName();

		// Where句の解析
		conditionParser = new ConditionParser((DatabaseMetaDataImpl)metaData);
		conditionParser.setTableName(tableName);
		updStmt.getWhere().accept(conditionParser);

		for(Column column : (List<Column>)updStmt.getColumns()) {
			SolrColumn solrColumn =((DatabaseMetaDataImpl)metaData).getSolrColumn(tableName, column.getColumnName());
			solrColumnNames.put(solrColumn.getSolrColumnName(), 1);
		}

		// SET句の解析
		expressionParser = new ExpressionParser();
		for(Expression expr : (List<Expression>)updStmt.getExpressions()) {
			expr.accept(expressionParser);
		}

		initParameters(conditionParser.getParameterSize() +
				expressionParser.getParameterSize());
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
