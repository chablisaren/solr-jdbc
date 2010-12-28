package com.google.code.solr_jdbc.impl;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.google.code.solr_jdbc.expression.Expression;
import com.google.code.solr_jdbc.expression.SolrColumn;
import com.google.code.solr_jdbc.message.DbException;
import com.google.code.solr_jdbc.message.ErrorCode;
import com.google.code.solr_jdbc.value.DataType;
import com.google.code.solr_jdbc.value.SolrType;


public class ResultSetMetaDataImpl implements ResultSetMetaData {
	private final String catalog;
	private final AbstractResultSet resultSet;
	private final List<Expression> expressions;
//	private final List<SolrColumn> solrColumns = new ArrayList<SolrColumn>();

	public ResultSetMetaDataImpl(AbstractResultSet resultSet, List<Expression> expressions, String catalog) {
		this.catalog = catalog;
		this.expressions = expressions;
		this.resultSet = resultSet;
	}

	/**
	 * カラムの名前から何番目のカラムかを検索する
	 *
	 * @param columnLabel カラムのラベル(ASで別名を割り当てている場合はそちらが優先される)
	 * @return index of column
	 * @throws SQLException
	 */
	public int findColumn(String columnLabel) throws SQLException{
		for(int i=0; i<expressions.size(); i++) {
			if (StringUtils.equalsIgnoreCase(expressions.get(i).getAlias(), columnLabel)
				|| StringUtils.equalsIgnoreCase(expressions.get(i).getColumnName(), columnLabel)) {
				return i+1; // parameterIndexは1始まりなので+1する
			}
		}
		throw new SQLException("column not found: "+columnLabel);
	}
	@Override
	public String getCatalogName(int column) throws SQLException {
		checkClosed();
		return catalog;
	}

	@Override
	public String getColumnClassName(int column) throws SQLException {
		column--;
		SolrType type = expressions.get(column).getType();
		return DataType.getTypeClassName(type);
	}

	@Override
	public int getColumnCount() throws SQLException {
		return expressions.size();
	}

	@Override
	public int getColumnDisplaySize(int column) throws SQLException {
		checkClosed();
		checkColumnIndex(column);
		return 0;
	}

	@Override
	public String getColumnLabel(int column) throws SQLException {
		checkClosed();
		checkColumnIndex(column);
		Expression expression = expressions.get(column - 1);
		String columnName = expression.getAlias();
		if(columnName != null)
			return columnName;
		return expression.getColumnName();
	}

	@Override
	public String getColumnName(int column) throws SQLException {
		checkClosed();
		checkColumnIndex(column);
		Expression solrColumn = expressions.get(column - 1);
		return solrColumn.getColumnName();
	}

	public String getSolrColumnName(int column) throws SQLException {
		checkClosed();
		checkColumnIndex(column);
		Expression solrColumn = expressions.get(column - 1);
		return solrColumn.getSolrColumnName();
	}

	public Expression getColumn(int column) throws SQLException {
		checkClosed();
		checkColumnIndex(column);
		return expressions.get(column - 1);
	}

	@Override
	public int getColumnType(int column) throws SQLException {
		checkClosed();
		checkColumnIndex(column);
		Expression solrColumn = expressions.get(column - 1);
		return DataType.getDataType(solrColumn.getType()).sqlType;
	}

	@Override
	public String getColumnTypeName(int column) throws SQLException {
		Expression solrColumn = expressions.get(column - 1);
		return DataType.getDataType(solrColumn.getType()).jdbc;
	}

	@Override
	public int getPrecision(int column) throws SQLException {
		// TODO 自動生成されたメソッド・スタブ
		return 0;
	}

	@Override
	public int getScale(int column) throws SQLException {
		// TODO 自動生成されたメソッド・スタブ
		return 0;
	}

	@Override
	public String getSchemaName(int column) throws SQLException {
		// TODO 自動生成されたメソッド・スタブ
		return null;
	}

	@Override
	public String getTableName(int column) throws SQLException {
		// TODO 自動生成されたメソッド・スタブ
		return null;
	}

	@Override
	public boolean isAutoIncrement(int column) throws SQLException {
		return false;
	}

	@Override
	public boolean isCaseSensitive(int column) throws SQLException {
		// TODO 自動生成されたメソッド・スタブ
		return false;
	}

	@Override
	public boolean isCurrency(int column) throws SQLException {
		// TODO 自動生成されたメソッド・スタブ
		return false;
	}

	@Override
	public boolean isDefinitelyWritable(int column) throws SQLException {
		// TODO 自動生成されたメソッド・スタブ
		return false;
	}

	@Override
	public int isNullable(int column) throws SQLException {
		// TODO 自動生成されたメソッド・スタブ
		return 0;
	}

	@Override
	public boolean isReadOnly(int column) throws SQLException {
		return true;
	}

	@Override
	public boolean isSearchable(int column) throws SQLException {
		// TODO 自動生成されたメソッド・スタブ
		return false;
	}

	@Override
	public boolean isSigned(int column) throws SQLException {
		// TODO 自動生成されたメソッド・スタブ
		return false;
	}

	@Override
	public boolean isWritable(int column) throws SQLException {
		return false;
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "isWrapperFor")
			.getSQLException();
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "unwrap")
			.getSQLException();
	}

	public List<Expression> getCountColumnList() {
		List<Expression> countColumns = new ArrayList<Expression>();
		for(Expression solrColumn : expressions) {
			if(solrColumn instanceof FunctionSolrColumn &&
				StringUtils.equals(((FunctionSolrColumn)solrColumn).getFunctionName(), "count")) {
				countColumns.add(solrColumn);
			}
		}
		return countColumns;
	}
	
	private void checkClosed() throws SQLException {
		if (resultSet != null) {
			resultSet.checkClosed();
		}
	}
	
	private void checkColumnIndex(int columnIndex) throws SQLException {
		if (columnIndex < 1 || columnIndex > getColumnCount()) {
			throw DbException.get(ErrorCode.INVALID_VALUE, "columnIndex:" + columnIndex);
		}
	}
}
