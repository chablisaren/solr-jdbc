package com.google.code.solr_jdbc.impl;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

public class CollectionResultSet implements ResultSet{
	private int recordIndex;
	private final List<List<Object>> data;
	private List<String> columns;

	public CollectionResultSet() {
		this.data = new ArrayList<List<Object>>();
		this.columns = new ArrayList<String>();
		this.recordIndex = -1;
	}

	protected void add(List<Object> record) {
		data.add(record);
	}

	public void setColumns(List<String> columnNames) {
		this.columns = columnNames;
	}
	@Override
	public boolean absolute(int i) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void afterLast() throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void beforeFirst() throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void cancelRowUpdates() throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void clearWarnings() throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void deleteRow() throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public int findColumn(String s) throws SQLException {
		int i=0;
		for(; i<columns.size(); i++) {
			if (s.equals(columns.get(i))) {
				break;
			}
		}
		if(i==columns.size()) {
			throw new SQLException("can't find column: " + s);
		}
		return i;
	}

	@Override
	public boolean first() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Array getArray(int i) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Array getArray(String s) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InputStream getAsciiStream(int i) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InputStream getAsciiStream(String s) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BigDecimal getBigDecimal(int i) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BigDecimal getBigDecimal(String s) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BigDecimal getBigDecimal(int i, int j) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BigDecimal getBigDecimal(String s, int i) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InputStream getBinaryStream(int i) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InputStream getBinaryStream(String s) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Blob getBlob(int i) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Blob getBlob(String s) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean getBoolean(int i) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean getBoolean(String s) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public byte getByte(int i) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public byte getByte(String s) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public byte[] getBytes(int i) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] getBytes(String s) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Reader getCharacterStream(int i) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Reader getCharacterStream(String s) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Clob getClob(int i) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Clob getClob(String s) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getConcurrency() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getCursorName() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Date getDate(int i) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Date getDate(String s) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Date getDate(int i, Calendar calendar) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Date getDate(String s, Calendar calendar) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public double getDouble(int i) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double getDouble(String s) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getFetchDirection() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getFetchSize() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public float getFloat(int i) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public float getFloat(String s) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getHoldability() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getInt(int i) throws SQLException {
		Object val = data.get(recordIndex).get(i);
		if (val == null)
			return 0;
		if (val instanceof Integer)
			return (Integer)val;

		try {
			return Integer.parseInt(val.toString());
		} catch (NumberFormatException e) {
			return 0;
		}
	}

	@Override
	public int getInt(String s) throws SQLException {
		int columnIdx = findColumn(s);
		return getInt(columnIdx);
	}

	@Override
	public long getLong(int i) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getLong(String s) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Reader getNCharacterStream(int i) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Reader getNCharacterStream(String s) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NClob getNClob(int i) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NClob getNClob(String s) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getNString(int i) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getNString(String s) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object getObject(int i) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object getObject(String s) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object getObject(int arg0, Map<String, Class<?>> arg1)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object getObject(String arg0, Map<String, Class<?>> arg1)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Ref getRef(int i) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Ref getRef(String s) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getRow() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public RowId getRowId(int i) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RowId getRowId(String s) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SQLXML getSQLXML(int i) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SQLXML getSQLXML(String s) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public short getShort(int i) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public short getShort(String s) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Statement getStatement() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getString(int i) throws SQLException {
		Object val = data.get(recordIndex).get(i);
		return (val == null) ? null : val.toString();
	}

	@Override
	public String getString(String s) throws SQLException {
		int columnIdx = findColumn(s);
		return getString(columnIdx);
	}

	@Override
	public Time getTime(int i) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Time getTime(String s) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Time getTime(int i, Calendar calendar) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Time getTime(String s, Calendar calendar) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Timestamp getTimestamp(int i) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Timestamp getTimestamp(String s) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Timestamp getTimestamp(int i, Calendar calendar) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Timestamp getTimestamp(String s, Calendar calendar)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getType() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public URL getURL(int i) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public URL getURL(String s) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InputStream getUnicodeStream(int i) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InputStream getUnicodeStream(String s) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void insertRow() throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean isAfterLast() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isBeforeFirst() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isClosed() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isFirst() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isLast() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean last() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void moveToCurrentRow() throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void moveToInsertRow() throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean next() throws SQLException {
		recordIndex += 1;
		if (recordIndex < data.size()) {
			return true;
		}
		return false;
	}

	@Override
	public boolean previous() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void refreshRow() throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean relative(int i) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean rowDeleted() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean rowInserted() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean rowUpdated() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setFetchDirection(int i) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setFetchSize(int i) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateArray(int i, Array array) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateArray(String s, Array array) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateAsciiStream(int i, InputStream inputstream)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateAsciiStream(String s, InputStream inputstream)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateAsciiStream(int i, InputStream inputstream, int j)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateAsciiStream(String s, InputStream inputstream, int i)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateAsciiStream(int i, InputStream inputstream, long l)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateAsciiStream(String s, InputStream inputstream, long l)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBigDecimal(int i, BigDecimal bigdecimal)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBigDecimal(String s, BigDecimal bigdecimal)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBinaryStream(int i, InputStream inputstream)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBinaryStream(String s, InputStream inputstream)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBinaryStream(int i, InputStream inputstream, int j)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBinaryStream(String s, InputStream inputstream, int i)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBinaryStream(int i, InputStream inputstream, long l)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBinaryStream(String s, InputStream inputstream, long l)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBlob(int i, Blob blob) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBlob(String s, Blob blob) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBlob(int i, InputStream inputstream) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBlob(String s, InputStream inputstream)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBlob(int i, InputStream inputstream, long l)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBlob(String s, InputStream inputstream, long l)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBoolean(int i, boolean flag) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBoolean(String s, boolean flag) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateByte(int i, byte byte0) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateByte(String s, byte byte0) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBytes(int i, byte[] abyte0) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateBytes(String s, byte[] abyte0) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateCharacterStream(int i, Reader reader) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateCharacterStream(String s, Reader reader)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateCharacterStream(int i, Reader reader, int j)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateCharacterStream(String s, Reader reader, int i)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateCharacterStream(int i, Reader reader, long l)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateCharacterStream(String s, Reader reader, long l)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateClob(int i, Clob clob) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateClob(String s, Clob clob) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateClob(int i, Reader reader) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateClob(String s, Reader reader) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateClob(int i, Reader reader, long l) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateClob(String s, Reader reader, long l) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateDate(int i, Date date) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateDate(String s, Date date) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateDouble(int i, double d) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateDouble(String s, double d) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateFloat(int i, float f) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateFloat(String s, float f) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateInt(int i, int j) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateInt(String s, int i) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateLong(int i, long l) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateLong(String s, long l) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateNCharacterStream(int i, Reader reader)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateNCharacterStream(String s, Reader reader)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateNCharacterStream(int i, Reader reader, long l)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateNCharacterStream(String s, Reader reader, long l)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateNClob(int i, NClob nclob) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateNClob(String s, NClob nclob) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateNClob(int i, Reader reader) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateNClob(String s, Reader reader) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateNClob(int i, Reader reader, long l) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateNClob(String s, Reader reader, long l)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateNString(int i, String s) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateNString(String s, String s1) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateNull(int i) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateNull(String s) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateObject(int i, Object obj) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateObject(String s, Object obj) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateObject(int i, Object obj, int j) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateObject(String s, Object obj, int i) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateRef(int i, Ref ref) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateRef(String s, Ref ref) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateRow() throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateRowId(int i, RowId rowid) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateRowId(String s, RowId rowid) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateSQLXML(int i, SQLXML sqlxml) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateSQLXML(String s, SQLXML sqlxml) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateShort(int i, short word0) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateShort(String s, short word0) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateString(int i, String s) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateString(String s, String s1) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateTime(int i, Time time) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateTime(String s, Time time) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateTimestamp(int i, Timestamp timestamp) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateTimestamp(String s, Timestamp timestamp)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean wasNull() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(StringUtils.join(columns, ',')).append('\n');
		for(List<Object> row : data) {
			sb.append(StringUtils.join(row, ',')).append('\n');
		}
		return sb.toString();
	}
}
