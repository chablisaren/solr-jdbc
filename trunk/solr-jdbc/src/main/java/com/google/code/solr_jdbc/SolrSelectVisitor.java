package com.google.code.solr_jdbc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.ColumnIndex;
import net.sf.jsqlparser.statement.select.ColumnReferenceVisitor;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.Union;

import org.apache.commons.lang.StringUtils;

import com.google.code.solr_jdbc.impl.DatabaseMetaDataImpl;
import com.google.code.solr_jdbc.impl.ResultSetMetaDataImpl;
import com.google.code.solr_jdbc.message.DbException;
import com.google.code.solr_jdbc.message.ErrorCode;
import com.google.code.solr_jdbc.parser.ConditionParser;
import com.google.code.solr_jdbc.parser.SelectItemFinder;
import com.google.code.solr_jdbc.value.SolrValue;


public class SolrSelectVisitor implements SelectVisitor, FromItemVisitor, ItemsListVisitor{

	private ConditionParser conditionParser;
	
	private String tableName;
	private final StringBuilder query;
	private final Map<String, String> solrOptions;
	private final DatabaseMetaDataImpl metaData;
	private final List<String> selectColumns;
	private ResultSetMetaDataImpl rsMetaData;
	private int parameterSize;
	private boolean hasGroupBy = false;

	public SolrSelectVisitor(DatabaseMetaDataImpl metaData) {
		this.query = new StringBuilder();
		this.selectColumns = new ArrayList<String>();
		this.solrOptions = new HashMap<String, String>();
		this.metaData = metaData;
	}

	public int getParameterSize() {
		return parameterSize;
	}

	public String getTableName() {
		return tableName;
	}

	public Map<String,String> getSolrOptions() {
		return solrOptions;
	}
	public List<String> getSelectColumns() {
		return selectColumns;
	}

	public ResultSetMetaDataImpl getResultSetMetaData() {
		return rsMetaData;
	}

	public String getQuery(SolrValue[] params) {
		String queryString;
		if(conditionParser == null) {
			// select all records if there is no WHERE clause.
			queryString = "id:@"+tableName+".*";
		} else {
			queryString = conditionParser.getQuery(params);
		}

		return queryString;
	}

	@Override
	public void visit(PlainSelect plainSelect) {
		plainSelect.getFromItem().accept(this);


		// 取得するカラムの解析
		SelectItemFinder selectItemFinder = new SelectItemFinder(tableName, metaData);
		for(Object obj : plainSelect.getSelectItems()) {
			((SelectItem)obj).accept(selectItemFinder);
		}
		rsMetaData = selectItemFinder.getResultSetMetaData();

		// Where句の解析
		if (plainSelect.getWhere()!=null) {
			conditionParser = new ConditionParser(metaData);
			conditionParser.setTableName(tableName);
			plainSelect.getWhere().accept(conditionParser);
		}

		// ORDER BYの解析
		if (plainSelect.getOrderByElements() != null) {
			parseOrderBy(plainSelect.getOrderByElements());
		}

		// GROUP BYの解析
		List<Column> groupByColumns = plainSelect.getGroupByColumnReferences();
		if (groupByColumns != null) {
			parseGroupBy(groupByColumns);
			hasGroupBy = true;
		}

		// Distinct
		Distinct distinct = plainSelect.getDistinct();
		if(distinct != null) {
			List<Distinct> distinctItems = distinct.getOnSelectItems();
		}
		
		// Limitの解析
		Limit limit = plainSelect.getLimit();
		if(limit != null) {
			solrOptions.put("start", Long.toString(limit.getOffset()));
			solrOptions.put("rows", Long.toString(limit.getRowCount()));
		}
		
		if(conditionParser != null)
			parameterSize = conditionParser.getParameterSize();
	}

	private void parseGroupBy(List<Column> groupByColumns) {
		for(Column column : groupByColumns) {
			solrOptions.put("facet", "true");
			SolrColumn solrColumn = metaData.getSolrColumn(tableName, column.getColumnName());
			solrOptions.put("facet.field", solrColumn.getSolrColumnName());
		}
	}

	@SuppressWarnings("unchecked")
	public void parseOrderBy(List orderByElements) {
		final List<String> sortColumns = new ArrayList<String>();
		for(Object elm : orderByElements) {
			final OrderByElement orderByElement = (OrderByElement)elm;
			orderByElement.getColumnReference().accept(new ColumnReferenceVisitor() {
				@Override
				public void visit(Column col) {
					SolrColumn solrColumn = metaData.getSolrColumn(tableName, col.getColumnName());
					String order = (orderByElement.isAsc()) ? "asc" : "desc";
					sortColumns.add(solrColumn.getSolrColumnName() + " " + order);
				}
				@Override
				public void visit(ColumnIndex colIndex) {
				}
			});
		}
		solrOptions.put("sort", StringUtils.join(sortColumns.iterator(), ","));
	}
	@Override
	public void visit(Union union) {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void visit(Table table){
		tableName = table.getName();
		if(metaData.getSolrColumns(tableName) == null) {
			throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND, tableName);
		}

	}

	@Override
	public void visit(SubSelect arg0) {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void visit(SubJoin arg0) {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}


	@Override
	public void visit(ExpressionList arg0) {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	public boolean hasGroupBy() {
		return hasGroupBy ;
	}

}
