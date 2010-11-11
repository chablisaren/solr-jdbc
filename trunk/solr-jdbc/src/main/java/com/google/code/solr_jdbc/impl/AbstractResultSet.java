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
import java.util.Calendar;
import java.util.Collection;
import java.util.Map;

import org.apache.solr.common.SolrDocumentList;

import com.google.code.solr_jdbc.SolrColumn;
import com.google.code.solr_jdbc.message.DbException;
import com.google.code.solr_jdbc.message.ErrorCode;
import com.google.code.solr_jdbc.value.DataType;
import com.google.code.solr_jdbc.value.SolrValue;
import com.google.code.solr_jdbc.value.ValueNull;


public abstract class AbstractResultSet implements ResultSet {
	protected SolrDocumentList docList;
	protected int docIndex = -1;
	protected ResultSetMetaDataImpl metaData;
	protected boolean isClosed = false;

	@Override
	public boolean absolute(int i) throws SQLException {
		checkClosed();
		if (0 <= i && i < docList.size()) {
			return false;
		}
		docIndex = i;
		return true;
	}

	@Override
	public void afterLast() throws SQLException {
		checkClosed();
		docIndex = docList.size();
	}

	@Override
	public void beforeFirst() throws SQLException {
		checkClosed();
		docIndex = -1;
	}

	@Override
	public void cancelRowUpdates() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void clearWarnings() throws SQLException {
		checkClosed();
	}

	@Override
	public void close() throws SQLException {
		this.docList = null;
		isClosed = true;
	}

	@Override
	public void deleteRow() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public int findColumn(String columnLabel) throws SQLException {
		checkClosed();
		return metaData.findColumn(columnLabel);
	}

	@Override
	public boolean first() throws SQLException {
		checkClosed();
		docIndex = 0;
		return true;
	}

	@Override
	public Array getArray(int columnIndex) throws SQLException {
		SolrValue v = get(columnIndex);
		return v == ValueNull.INSTANCE ? null : new ArrayImpl(v);
	}

	@Override
	public Array getArray(String columnLabel) throws SQLException {
		SolrValue v = get(columnLabel);

		return v == ValueNull.INSTANCE ? null : new ArrayImpl(v);
	}

	@Override
	public InputStream getAsciiStream(int columnIndex) throws SQLException {
		return null;
	}

	@Override
	public InputStream getAsciiStream(String s) throws SQLException {
		if (isClosed)
			throw new SQLException("既に閉じられています");

		return null;
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		return get(columnIndex).getBigDecimal();
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
		return get(columnLabel).getBigDecimal();
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex, int j) throws SQLException {
		checkClosed();
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED); 
	}

	@Override
	public BigDecimal getBigDecimal(String s, int i) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED); 
	}

	@Override
	public InputStream getBinaryStream(int i) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED); 
	}

	@Override
	public InputStream getBinaryStream(String s) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED); 
	}

	@Override
	public Blob getBlob(int i) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED); 
	}

	@Override
	public Blob getBlob(String s) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED); 
	}

	@Override
	public boolean getBoolean(int columnIndex) throws SQLException {
		checkClosed();
		return get(columnIndex).getBoolean();
	}

	@Override
	public boolean getBoolean(String columnLabel) throws SQLException {
		checkClosed();
		return get(columnLabel).getBoolean();
	}

	@Override
	public byte getByte(int i) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public byte getByte(String s) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public byte[] getBytes(int i) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public byte[] getBytes(String s) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public Reader getCharacterStream(int i) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public Reader getCharacterStream(String s) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public Clob getClob(int i) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public Clob getClob(String s) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public int getConcurrency() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public String getCursorName() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public Date getDate(int columnIndex) throws SQLException {
		return get(columnIndex).getDate();
	}

	@Override
	public Date getDate(String columnLabel) throws SQLException {
		return get(columnLabel).getDate();
	}

	@Override
	public Date getDate(int columnIndex, Calendar calendar) throws SQLException {
		//TODO
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public Date getDate(String columnLabel, Calendar calendar) throws SQLException {
		// TODO Auto-generated method stub
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public double getDouble(int columnIndex) throws SQLException {
		return get(columnIndex).getDouble();
	}

	@Override
	public double getDouble(String columnLabel) throws SQLException {
		return get(columnLabel).getDouble();
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
	public int getInt(int columnIndex) throws SQLException {
		return get(columnIndex).getInt();
	}

	@Override
	public int getInt(String columnLabel) throws SQLException {
		return get(columnLabel).getInt();
	}

	@Override
	public long getLong(int columnIndex) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public long getLong(String s) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		return metaData;
	}

	@Override
	public Reader getNCharacterStream(int i) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public Reader getNCharacterStream(String s) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public NClob getNClob(int i) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public NClob getNClob(String s) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public String getNString(int columnIndex) throws SQLException {
		return get(columnIndex).getString();
	}

	@Override
	public String getNString(String columnLabel) throws SQLException {
		return get(columnLabel).getString();
	}

	@Override
	public Object getObject(int columnIndex) throws SQLException {
		SolrValue v = get(columnIndex);
		return v.getObject();
	}

	@Override
	public Object getObject(String columnLabel) throws SQLException {
		return get(columnLabel).getObject();
	}

	@Override
	public Object getObject(int arg0, Map<String, Class<?>> arg1)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public Object getObject(String arg0, Map<String, Class<?>> arg1)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public Ref getRef(int i) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public Ref getRef(String s) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public int getRow() throws SQLException {
		if (docIndex < 0 || docIndex >= docList.size()) {
			return 0;
		}
		return docIndex+1;
	}

	@Override
	public RowId getRowId(int columnIndex) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public RowId getRowId(String columnLabel) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public SQLXML getSQLXML(int columnIndex) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public SQLXML getSQLXML(String columnLabel) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public short getShort(int columnIndex) throws SQLException {
		return (short)get(columnIndex).getInt();
	}

	@Override
	public short getShort(String columnLabel) throws SQLException {
		return (short)get(columnLabel).getInt();
	}

	@Override
	public Statement getStatement() throws SQLException {
		// TODO 
		return null;
	}

	@Override
	public String getString(int columnIndex) throws SQLException {
		return get(columnIndex).getString();
	}

	@Override
	public String getString(String columnLabel) throws SQLException {
		return get(columnLabel).getString();
	}

	@Override
	public Time getTime(int columnIndex) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public Time getTime(String columnLabel) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public Time getTime(int columnIndex, Calendar calendar) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public Time getTime(String columnLabel, Calendar calendar) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		return get(columnIndex).getTimestamp();
	}

	@Override
	public Timestamp getTimestamp(String columnLabel) throws SQLException {
		return get(columnLabel).getTimestamp();
	}

	@Override
	public Timestamp getTimestamp(int i, Calendar calendar) throws SQLException {
		// TODO
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public Timestamp getTimestamp(String s, Calendar calendar)
			throws SQLException {
		// TODO
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public int getType() throws SQLException {
		// TODO
		return 0;
	}

	@Override
	public URL getURL(int columnIndex) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public URL getURL(String columnLabel) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public InputStream getUnicodeStream(int columnIndex) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public InputStream getUnicodeStream(String columnLabel) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void insertRow() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public boolean isAfterLast() throws SQLException {
		return (docIndex >= docList.size());
	}

	@Override
	public boolean isBeforeFirst() throws SQLException {
		return (docIndex < 0);
	}

	@Override
	public boolean isClosed() throws SQLException {
		return isClosed;
	}

	@Override
	public boolean isFirst() throws SQLException {
		return docIndex == 0;
	}

	@Override
	public boolean isLast() throws SQLException {
		return docIndex == docList.size() - 1;
	}

	@Override
	public boolean last() throws SQLException {
		// TODO TYPE_FORWARD_ONLYのときはSQLExceptionをだす
		docIndex = docList.size() - 1;
		return true;
	}

	@Override
	public void moveToCurrentRow() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void moveToInsertRow() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public boolean next() throws SQLException {
		checkClosed();

		docIndex +=1;
		if (docIndex >= docList.size()) {
			docIndex = docList.size() - 1;
			return false;
		}

		return true;
	}

	@Override
	public boolean previous() throws SQLException {
		// TODO スクロールが不可能な場合はSQLException
		checkClosed();
		
		docIndex -= 1;
		if (docIndex < 0) {
			docIndex = 0;
			return false;
		}
		return true;
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
		return false;
	}

	@Override
	public boolean rowInserted() throws SQLException {
		return false;
	}

	@Override
	public boolean rowUpdated() throws SQLException {
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
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateArray(String s, Array array) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateAsciiStream(int i, InputStream inputstream)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateAsciiStream(String s, InputStream inputstream)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateAsciiStream(int i, InputStream inputstream, int j)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateAsciiStream(String s, InputStream inputstream, int i)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateAsciiStream(int i, InputStream inputstream, long l)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateAsciiStream(String s, InputStream inputstream, long l)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateBigDecimal(int i, BigDecimal bigdecimal)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateBigDecimal(String s, BigDecimal bigdecimal)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateBinaryStream(int i, InputStream inputstream)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateBinaryStream(String s, InputStream inputstream)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateBinaryStream(int i, InputStream inputstream, int j)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateBinaryStream(String s, InputStream inputstream, int i)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateBinaryStream(int i, InputStream inputstream, long l)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateBinaryStream(String s, InputStream inputstream, long l)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateBlob(int i, Blob blob) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateBlob(String s, Blob blob) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateBlob(int i, InputStream inputstream) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateBlob(String s, InputStream inputstream)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateBlob(int i, InputStream inputstream, long l)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateBlob(String s, InputStream inputstream, long l)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateBoolean(int i, boolean flag) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateBoolean(String s, boolean flag) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateByte(int i, byte byte0) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateByte(String s, byte byte0) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateBytes(int i, byte[] abyte0) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateBytes(String s, byte[] abyte0) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateCharacterStream(int i, Reader reader) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateCharacterStream(String s, Reader reader)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateCharacterStream(int i, Reader reader, int j)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateCharacterStream(String s, Reader reader, int i)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateCharacterStream(int i, Reader reader, long l)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateCharacterStream(String s, Reader reader, long l)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateClob(int i, Clob clob) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateClob(String s, Clob clob) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateClob(int i, Reader reader) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateClob(String s, Reader reader) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateClob(int i, Reader reader, long l) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateClob(String s, Reader reader, long l) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateDate(int i, Date date) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateDate(String s, Date date) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateDouble(int i, double d) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateDouble(String s, double d) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateFloat(int i, float f) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateFloat(String s, float f) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateInt(int i, int j) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateInt(String s, int i) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateLong(int i, long l) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateLong(String s, long l) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateNCharacterStream(int i, Reader reader)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateNCharacterStream(String s, Reader reader)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateNCharacterStream(int i, Reader reader, long l)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateNCharacterStream(String s, Reader reader, long l)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateNClob(int i, NClob nclob) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateNClob(String s, NClob nclob) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateNClob(int i, Reader reader) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateNClob(String s, Reader reader) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateNClob(int i, Reader reader, long l) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateNClob(String s, Reader reader, long l)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateNString(int i, String s) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateNString(String s, String s1) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateNull(int i) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateNull(String s) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateObject(int i, Object obj) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateObject(String s, Object obj) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateObject(int i, Object obj, int j) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateObject(String s, Object obj, int i) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateRef(int i, Ref ref) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateRef(String s, Ref ref) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateRow() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateRowId(int i, RowId rowid) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateRowId(String s, RowId rowid) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateSQLXML(int i, SQLXML sqlxml) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateSQLXML(String s, SQLXML sqlxml) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateShort(int i, short word0) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateShort(String s, short word0) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateString(int i, String s) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateString(String s, String s1) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateTime(int i, Time time) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateTime(String s, Time time) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void updateTimestamp(int i, Timestamp timestamp) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
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
	public boolean isWrapperFor(Class<?> arg0) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public <T> T unwrap(Class<T> arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	private SolrValue get(int columnIndex) throws SQLException{
		checkClosed();
		SolrColumn column = metaData.getColumn(columnIndex);

		String columnName = (column.getType() == null)?column.getResultName():column.getSolrColumnName();

		Object x;
		Collection<Object> objs = docList.get(docIndex).getFieldValues(columnName);
		if (objs == null) {
			x = null;
		} else if (objs.size() == 1) {
			x = objs.toArray()[0];
		} else {
			x = objs.toArray();
		}

		return DataType.convertToValue(x);
	}

	private SolrValue get(String columnLabel) throws SQLException {
		int columnIndex = findColumn(columnLabel);
		return get(columnIndex);
	}

	private void checkClosed() throws SQLException {
		if (isClosed)
			throw new SQLException("既に閉じられています");
	}
}
