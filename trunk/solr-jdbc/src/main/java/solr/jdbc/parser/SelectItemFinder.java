package solr.jdbc.parser;

import java.sql.SQLException;
import java.util.List;

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
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import net.sf.jsqlparser.statement.select.SubSelect;

import org.apache.commons.lang.StringUtils;

import solr.jdbc.SolrColumn;
import solr.jdbc.impl.DatabaseMetaDataImpl;
import solr.jdbc.impl.FunctionSolrColumn;
import solr.jdbc.impl.ResultSetMetaDataImpl;
import solr.jdbc.message.DbException;
import solr.jdbc.message.ErrorCode;

public class SelectItemFinder implements SelectItemVisitor, ExpressionVisitor {
	private final String tableName;
	private final DatabaseMetaDataImpl dbMetaData;
	private final ResultSetMetaDataImpl rsMetaData;

	private SolrColumn currentColumn;

	public SelectItemFinder(String tableName, DatabaseMetaDataImpl metaData) {
		this.tableName  = tableName;
		this.dbMetaData = metaData;
		this.rsMetaData = new ResultSetMetaDataImpl();
	}

	public ResultSetMetaDataImpl getResultSetMetaData() {
		return rsMetaData;
	}

	@Override
	public void visit(AllColumns cols) {
		try {
			List<SolrColumn> solrColumns = dbMetaData.getSolrColumns(tableName);
			for(SolrColumn solrColumn : solrColumns) {
				rsMetaData.addColumn(solrColumn);
			}
		} catch (SQLException e) {
			throw DbException.get(ErrorCode.GENERAL_ERROR, "Metadata Not Found");
		}
	}

	@Override
	public void visit(AllTableColumns cols) {
	}

	@Override
	public void visit(SelectExpressionItem expr) {
		expr.getExpression().accept(this);
		if (currentColumn != null) {
			currentColumn.setAlias(expr.getAlias());
			rsMetaData.addColumn(currentColumn);
			currentColumn = null;
		}
	}

	@Override
	public void visit(NullValue arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Function func) {
		currentColumn = new FunctionSolrColumn(func);
	}

	@Override
	public void visit(InverseExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(JdbcParameter arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(DoubleValue arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(LongValue arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(DateValue arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(TimeValue arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(TimestampValue arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Parenthesis arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(StringValue arg0) {
		// TODO Auto-generated method stub

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
		// TODO Auto-generated method stub

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
	public void visit(Column column) {
		try {
			List<SolrColumn> solrColumns = dbMetaData.getSolrColumns(tableName);
			for(SolrColumn solrColumn : solrColumns) {
				if(StringUtils.equals(solrColumn.getColumnName(), column.getColumnName())) {
					currentColumn = solrColumn;
					break;
				}
			}
		} catch (SQLException e) {
			throw DbException.get(ErrorCode.GENERAL_ERROR, "Metadata Not Found");
		}

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
