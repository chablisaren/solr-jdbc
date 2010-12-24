package com.google.code.solr_jdbc.parser;

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

import com.google.code.solr_jdbc.message.DbException;
import com.google.code.solr_jdbc.message.ErrorCode;
import com.google.code.solr_jdbc.value.SolrValue;
import com.google.code.solr_jdbc.value.ValueDouble;
import com.google.code.solr_jdbc.value.ValueLong;
import com.google.code.solr_jdbc.value.ValueNull;
import com.google.code.solr_jdbc.value.ValueString;

public class ExpressionParser implements ExpressionVisitor {

	private SolrValue value;
	private boolean isParameter;
	
	public ExpressionParser() {
	}

	public SolrValue getValue() {
		return value;
	}
	
	public boolean isParameter() {
		return isParameter;
	}
	
	@Override
	public void visit(NullValue val) {
		value = ValueNull.INSTANCE;
	}

	@Override
	public void visit(Function func) {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, func.getName());
	}

	@Override
	public void visit(InverseExpression val) {
		throw DbException.get(ErrorCode.SYNTAX_ERROR);
	}

	@Override
	public void visit(JdbcParameter param) {
		isParameter = true;
	}

	@Override
	public void visit(DoubleValue val) {
		value = ValueDouble.get(val.getValue());
	}

	@Override
	public void visit(LongValue val) {
		value = ValueLong.get(val.getValue());
	}

	@Override
	public void visit(DateValue val) {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "Date Literal:"+val);
	}

	@Override
	public void visit(TimeValue val) {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "Time Literal:"+val);
	}

	@Override
	public void visit(TimestampValue val) {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "Timestamp Literal:"+val);
	}

	@Override
	public void visit(Parenthesis arg0) {
		throw DbException.get(ErrorCode.SYNTAX_ERROR);
	}

	@Override
	public void visit(StringValue val) {
		value = ValueString.get(val.getValue());
	}

	@Override
	public void visit(Addition arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Division arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Multiplication arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Subtraction arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(AndExpression arg0) {
		throw DbException.get(ErrorCode.SYNTAX_ERROR);
	}

	@Override
	public void visit(OrExpression arg0) {
		throw DbException.get(ErrorCode.SYNTAX_ERROR);
	}

	@Override
	public void visit(Between arg0) {
		throw DbException.get(ErrorCode.SYNTAX_ERROR);
	}

	@Override
	public void visit(EqualsTo arg0) {
		throw DbException.get(ErrorCode.SYNTAX_ERROR);
	}

	@Override
	public void visit(GreaterThan arg0) {
		throw DbException.get(ErrorCode.SYNTAX_ERROR);
	}

	@Override
	public void visit(GreaterThanEquals arg0) {
		throw DbException.get(ErrorCode.SYNTAX_ERROR);
	}

	@Override
	public void visit(InExpression arg0) {
		throw DbException.get(ErrorCode.SYNTAX_ERROR);
	}

	@Override
	public void visit(IsNullExpression arg0) {
		throw DbException.get(ErrorCode.SYNTAX_ERROR);
	}

	@Override
	public void visit(LikeExpression arg0) {
		throw DbException.get(ErrorCode.SYNTAX_ERROR);
	}

	@Override
	public void visit(MinorThan arg0) {
		throw DbException.get(ErrorCode.SYNTAX_ERROR);
	}

	@Override
	public void visit(MinorThanEquals arg0) {
		throw DbException.get(ErrorCode.SYNTAX_ERROR);
	}

	@Override
	public void visit(NotEqualsTo arg0) {
		throw DbException.get(ErrorCode.SYNTAX_ERROR);
	}

	@Override
	public void visit(Column arg0) {
		throw DbException.get(ErrorCode.SYNTAX_ERROR);
	}

	@Override
	public void visit(SubSelect arg0) {
		throw DbException.get(ErrorCode.SYNTAX_ERROR);
	}

	@Override
	public void visit(CaseExpression arg0) {
		throw DbException.get(ErrorCode.SYNTAX_ERROR);
	}

	@Override
	public void visit(WhenClause arg0) {
		throw DbException.get(ErrorCode.SYNTAX_ERROR);
	}

	@Override
	public void visit(ExistsExpression arg0) {
		throw DbException.get(ErrorCode.SYNTAX_ERROR);
	}

	@Override
	public void visit(AllComparisonExpression arg0) {
		throw DbException.get(ErrorCode.SYNTAX_ERROR);
	}

	@Override
	public void visit(AnyComparisonExpression arg0) {
		throw DbException.get(ErrorCode.SYNTAX_ERROR);
	}

}
