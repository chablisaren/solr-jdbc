package solr.jdbc.impl;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import solr.jdbc.SolrColumn;
import solr.jdbc.value.DataType;

public class ResultSetMetaDataImpl implements ResultSetMetaData {
	private List<SolrColumn> solrColumns = new ArrayList<SolrColumn>();

	public void addColumn(SolrColumn solrColumn) {
		solrColumns.add(solrColumn);
	}

	public int findColumn(String columnLabel) throws SQLException{
		for(int i=0; i<solrColumns.size(); i++) {
			if (StringUtils.equalsIgnoreCase(solrColumns.get(i).getColumnName(), columnLabel)) {
				return i;
			}
		}
		throw new SQLException("column not found: "+columnLabel);
	}
	@Override
	public String getCatalogName(int column) throws SQLException {
		// TODO 自動生成されたメソッド・スタブ
		return null;
	}

	@Override
	public String getColumnClassName(int column) throws SQLException {
		// TODO 自動生成されたメソッド・スタブ
		return null;
	}

	@Override
	public int getColumnCount() throws SQLException {
		return solrColumns.size();
	}

	@Override
	public int getColumnDisplaySize(int column) throws SQLException {
		// TODO 自動生成されたメソッド・スタブ
		return 0;
	}

	@Override
	public String getColumnLabel(int column) throws SQLException {
		SolrColumn solrColumn = solrColumns.get(column);
		return solrColumn.getSolrColumnName();
	}

	@Override
	public String getColumnName(int column) throws SQLException {
		SolrColumn solrColumn = solrColumns.get(column);
		return solrColumn.getSolrColumnName();
	}

	@Override
	public int getColumnType(int column) throws SQLException {
		SolrColumn solrColumn = solrColumns.get(column);
		return DataType.getDataType(solrColumn.getType()).sqlType;
	}

	@Override
	public String getColumnTypeName(int column) throws SQLException {
		SolrColumn solrColumn = solrColumns.get(column);
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
		// TODO 自動生成されたメソッド・スタブ
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
		// TODO 自動生成されたメソッド・スタブ
		return false;
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
		// TODO 自動生成されたメソッド・スタブ
		return false;
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		// TODO 自動生成されたメソッド・スタブ
		return false;
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		// TODO 自動生成されたメソッド・スタブ
		return null;
	}

	public String getCountColumn() {
		// TODO Auto-generated method stub
		return null;
	}

}
