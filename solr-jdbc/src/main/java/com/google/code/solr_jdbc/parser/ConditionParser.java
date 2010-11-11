package com.google.code.solr_jdbc.parser;

import java.sql.Date;

import com.google.code.solr_jdbc.SolrColumn;
import com.google.code.solr_jdbc.impl.DatabaseMetaDataImpl;
import com.google.code.solr_jdbc.message.DbException;
import com.google.code.solr_jdbc.message.ErrorCode;
import com.google.code.solr_jdbc.value.SolrValue;
import com.google.code.solr_jdbc.value.ValueDate;
import com.google.code.solr_jdbc.value.ValueDouble;
import com.google.code.solr_jdbc.value.ValueLong;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;

public class ConditionParser implements ExpressionVisitor {

	private final StringBuilder query;
	private int parameterSize;
	private String tableName;
	private final DatabaseMetaDataImpl metaData;

	public ConditionParser(DatabaseMetaDataImpl metaData) {
		this.metaData = metaData;
		query = new StringBuilder();
		parameterSize = 0;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public int getParameterSize() {
		return parameterSize;
	}

	public String getQuery(SolrValue[] params) {
		String queryString;
		if(query.length() == 0) {
			queryString = "id:@"+tableName+".*";
		} else {
			queryString = query.toString();
		}

		StringBuilder sb = new StringBuilder();
		int i=0;
		int paramIndex=0;
		while(true) {
			int j = queryString.indexOf("{}", i);
			if (j < 0) break;
			sb.append(queryString.substring(i, j));
			sb.append(params[paramIndex++].getQueryString());
			i = j+2;
		}
		if(i<queryString.length()) {
			sb.append(queryString.substring(i));
		}

		return sb.toString();
	}

	@Override
	public void visit(SubSelect arg0) {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void visit(NullValue arg0) {
		query.append("\"\"");
	}

	@Override
	public void visit(Function arg0) {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void visit(InverseExpression arg0) {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void visit(JdbcParameter ph) {
		parameterSize += 1;
 		query.append("{}");
	}

	@Override
	public void visit(DoubleValue value) {
		query.append(ValueDouble.get(value.getValue()).getQueryString());
	}

	@Override
	public void visit(LongValue value) {
		query.append(ValueLong.get(value.getValue()).getQueryString());
	}

	@Override
	public void visit(DateValue value) {
		ValueDate d = ValueDate.get(value.getValue());
		query.append(d.getQueryString());
	}

	@Override
	public void visit(TimeValue value) {
		query.append(value.getValue().toString());
	}

	@Override
	public void visit(TimestampValue value) {
		ValueDate d = ValueDate.get(new Date(value.getValue().getTime()));
		query.append(d.getQueryString());
	}

	@Override
	public void visit(Parenthesis expr) {
		query.append("(");
		expr.getExpression().accept(this);
		query.append(")");
	}

	@Override
	public void visit(StringValue value) {
		query.append(value.getValue());
	}

	@Override
	public void visit(Addition arg0) {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void visit(Division arg0) {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void visit(Multiplication arg0) {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void visit(Subtraction arg0) {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void visit(AndExpression expr) {
		expr.getLeftExpression().accept(this);
		query.append(" AND ");
		expr.getRightExpression().accept(this);
	}

	@Override
	public void visit(OrExpression expr) {
		expr.getLeftExpression().accept(this);
		query.append(" OR ");
		expr.getRightExpression().accept(this);
	}

	@Override
	public void visit(Between expr) {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void visit(EqualsTo expr) {
		expr.getLeftExpression().accept(this);
		query.append(":");
		expr.getRightExpression().accept(this);
	}

	@Override
	public void visit(GreaterThan expr) {
		expr.getLeftExpression().accept(this);
		query.append(":{");
		expr.getRightExpression().accept(this);
		query.append(" TO *} ");
	}

	@Override
	public void visit(GreaterThanEquals expr) {
		expr.getLeftExpression().accept(this);
		query.append(":[");
		expr.getRightExpression().accept(this);
		query.append(" TO *] ");
	}

	@Override
	public void visit(InExpression arg0) {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void visit(IsNullExpression arg0) {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void visit(LikeExpression arg0) {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void visit(MinorThan expr) {
		expr.getLeftExpression().accept(this);
		query.append(":{* TO ");
		expr.getRightExpression().accept(this);
		query.append("}");
	}

	@Override
	public void visit(MinorThanEquals expr) {
		expr.getLeftExpression().accept(this);
		query.append(":[* TO ");
		expr.getRightExpression().accept(this);
		query.append("]");
	}

	@Override
	public void visit(NotEqualsTo arg0) {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void visit(Column column) {
		SolrColumn solrColumn = metaData.getSolrColumn(tableName, column.getColumnName());
		if (solrColumn == null) {
			throw DbException.get(ErrorCode.COLUMN_NOT_FOUND, column.getColumnName());
		}
		query.append(solrColumn.getSolrColumnName());
	}

	@Override
	public void visit(CaseExpression arg0) {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void visit(WhenClause arg0) {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void visit(ExistsExpression arg0) {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void visit(AllComparisonExpression arg0) {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void visit(AnyComparisonExpression arg0) {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}


}
