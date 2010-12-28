package com.google.code.solr_jdbc.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import com.google.code.solr_jdbc.command.Command;
import com.google.code.solr_jdbc.command.CommandFactory;
import com.google.code.solr_jdbc.expression.Parameter;
import com.google.code.solr_jdbc.message.DbException;
import com.google.code.solr_jdbc.message.ErrorCode;
import com.google.code.solr_jdbc.value.DataType;
import com.google.code.solr_jdbc.value.SolrValue;
import com.google.code.solr_jdbc.value.ValueBoolean;
import com.google.code.solr_jdbc.value.ValueDate;
import com.google.code.solr_jdbc.value.ValueDecimal;
import com.google.code.solr_jdbc.value.ValueDouble;
import com.google.code.solr_jdbc.value.ValueNull;
import com.google.code.solr_jdbc.value.ValueString;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;

/**
 * Solr PreparedStatement
 *
 * @author kawasima
 */
public class PreparedStatementImpl extends StatementImpl implements PreparedStatement {
	private Statement statement;
	private List<SolrValue[]> batchParameters;
	private Command command;

	protected PreparedStatementImpl(SolrConnection conn, String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		super(conn, resultSetType, resultSetConcurrency);
		CCJSqlParserManager pm = new CCJSqlParserManager();
		try {
			statement = pm.parse(new StringReader(sql));
			this.command = CommandFactory.getCommand(statement);
			command.setConnection(conn);
			command.parse();
		} catch (JSQLParserException e) {
			throw DbException.get(ErrorCode.SYNTAX_ERROR, e.getMessage());
		}
	}

	@Override
	public void addBatch() throws SQLException {
		checkClosed();
		List<Parameter> parameters = command.getParameters();
		int size = parameters.size();
		SolrValue[] set = new SolrValue[size];
		for (int i=0; i<size; i++) {
			Parameter param = parameters.get(i);
			set[i] = param.getValue();
		}
		if (batchParameters == null) {
			batchParameters = new ArrayList<SolrValue[]>();
		}
		batchParameters.add(set);
	}

	@Override
	public void clearParameters() throws SQLException {
		List<Parameter> parameters = command.getParameters();
		for(int i=0, size=parameters.size(); i<size; i++) {
			Parameter param = parameters.get(i);
			param.setValue(null);
		}
	}

	@Override
	public boolean execute() throws SQLException {
		checkClosed();
		boolean returnsResultSet;
		try {
			if(command.isQuery()) {
				returnsResultSet = true;
				resultSet = command.executeQuery();
			} else {
				returnsResultSet = false;
				updateCount = command.executeUpdate();
			}
		} catch(DbException e) {
			throw e.getSQLException();
		}
		return returnsResultSet;
	}

	@Override
	public ResultSet executeQuery() throws SQLException {
		checkClosed();
		try {
			resultSet = command.executeQuery();
			
			return resultSet;
		} catch (DbException e) {
			throw e.getSQLException();
		}
	}

	@Override
	public int executeUpdate() throws SQLException {
		checkClosed();
		try {
			return command.executeUpdate();
		} catch (DbException e) {
			throw e.getSQLException();
		}
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		checkClosed();
		if(resultSet == null) {
			return null;
		}
		return resultSet.getMetaData();
	}

	@Override
	public ParameterMetaData getParameterMetaData() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void setArray(int parameterIndex, Array x) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void setAsciiStream(int i, InputStream in) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void setAsciiStream(int arg0, InputStream arg1, int arg2)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void setAsciiStream(int arg0, InputStream arg1, long arg2)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
		SolrValue value = ValueDecimal.get(x);
		setParameter(parameterIndex, value);
	}

	@Override
	public void setBinaryStream(int arg0, InputStream arg1) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void setBinaryStream(int arg0, InputStream arg1, int arg2)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void setBinaryStream(int arg0, InputStream arg1, long arg2)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void setBlob(int arg0, Blob arg1) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void setBlob(int arg0, InputStream arg1) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void setBlob(int arg0, InputStream arg1, long arg2)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void setBoolean(int parameterIndex, boolean x) throws SQLException {
		SolrValue value = ValueBoolean.get(x);
		setParameter(parameterIndex, value);
	}

	@Override
	public void setByte(int arg0, byte arg1) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void setBytes(int arg0, byte[] arg1) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader r) throws SQLException {
		setClob(parameterIndex, r);
	}

	@Override
	public void setCharacterStream(int arg0, Reader arg1, int arg2)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void setCharacterStream(int arg0, Reader arg1, long arg2)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void setClob(int parameterIndex, Clob clob) throws SQLException {
		Reader r = clob.getCharacterStream();
		setClob(parameterIndex, r);
	}

	@Override
	public void setClob(int parameterIndex, Reader r) throws SQLException {
		char[] cbuf = new char[4096];
		StringBuilder sb = new StringBuilder();
		while(true) {
			try {
				int c = r.read(cbuf);
				if(c == 0)
					break;
				sb.append(cbuf);
			} catch(IOException e) {
				throw DbException.get(ErrorCode.IO_EXCEPTION, e);
			}
		}
		SolrValue value = ValueString.get(sb.toString());
		setParameter(parameterIndex, value);
	}

	@Override
	public void setClob(int parameterIndex, Reader r, long arg2) throws SQLException {
		//TODO
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void setDate(int parameterIndex, Date x) throws SQLException {
		SolrValue value = ValueDate.get(x);
		setParameter(parameterIndex, value);
	}

	@Override
	public void setDate(int parameterIndex, Date x, Calendar arg2) throws SQLException {
		SolrValue value = ValueDate.get(x);
		setParameter(parameterIndex, value);
	}

	@Override
	public void setDouble(int parameterIndex, double x) throws SQLException {
		SolrValue value = ValueDouble.get(x);
		setParameter(parameterIndex, value);
	}

	@Override
	public void setFloat(int parameterIndex, float x) throws SQLException {
		SolrValue value = ValueDouble.get(x);
		setParameter(parameterIndex, value);
	}

	@Override
	public void setInt(int paramterIndex, int x) throws SQLException {
		SolrValue value = ValueDecimal.get(new BigDecimal(x));
		setParameter(paramterIndex, value);
	}

	@Override
	public void setLong(int paramterIndex, long x) throws SQLException {
		SolrValue value = ValueDecimal.get(new BigDecimal(x));
		setParameter(paramterIndex, value);
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader r) throws SQLException {
		setClob(parameterIndex, r);
	}

	@Override
	public void setNCharacterStream(int arg0, Reader arg1, long arg2)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void setNClob(int parameterIndex, NClob nclob) throws SQLException {
		Reader r = nclob.getCharacterStream();
		setClob(parameterIndex, r);
	}

	@Override
	public void setNClob(int parameterIndex, Reader r) throws SQLException {
		setClob(parameterIndex, r);
	}

	@Override
	public void setNClob(int arg0, Reader arg1, long arg2) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void setNString(int parameterIndex, String s) throws SQLException {
		ValueString value = ValueString.get(s);
		setParameter(parameterIndex, value);
	}

	@Override
	public void setNull(int parameterIndex, int sqlType) throws SQLException {
		setParameter(parameterIndex, ValueNull.INSTANCE);
	}

	@Override
	public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
		setParameter(parameterIndex, ValueNull.INSTANCE);
	}

	@Override
	public void setObject(int parameterIndex, Object x) throws SQLException {
		if (x == null) {
			setParameter(parameterIndex, ValueNull.INSTANCE);
		} else {
			setParameter(parameterIndex, DataType.convertToValue(x));
		}
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
		setObject(parameterIndex, x);
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType, int scale)
			throws SQLException {
		setObject(parameterIndex, x);
	}

	@Override
	public void setRef(int arg0, Ref arg1) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void setRowId(int arg0, RowId arg1) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void setSQLXML(int arg0, SQLXML arg1) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED);
	}

	@Override
	public void setShort(int arg0, short x) throws SQLException {
	}

	@Override
	public void setString(int parameterIndex, String x) throws SQLException {
		SolrValue value = ValueString.get(x);
		setParameter(parameterIndex, value);
	}

	@Override
	public void setTime(int parameterIndex, Time x) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "setTime")
			.getSQLException();
	}

	@Override
	public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "setTime")
			.getSQLException();
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
		Date d = new Date(x.getTime());
		SolrValue value = ValueDate.get(d);
		setParameter(parameterIndex, value);
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "setTimestamp")
			.getSQLException();
	}

	@Override
	public void setURL(int parameterIndex, URL x) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "setURL")
			.getSQLException();
	}

	@Override
	public void setUnicodeStream(int parameterIndex, InputStream arg1, int arg2)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "setUnicodeStream")
			.getSQLException();
	}

	@Override
	public void addBatch(String sql) throws SQLException {
		throw DbException.get(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT, "addBatch")
			.getSQLException();
	}

	@Override
	public void clearBatch() throws SQLException {
		checkClosed();
		batchParameters = null;
	}

	@Override
	public boolean execute(String sql) throws SQLException {
		throw DbException.get(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT, "execute")
			.getSQLException();
	}

	@Override
	public boolean execute(String sql, int arg1) throws SQLException {
		throw DbException.get(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT, "execute")
			.getSQLException();
	}

	@Override
	public boolean execute(String sql, int[] arg1) throws SQLException {
		throw DbException.get(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT, "execute")
			.getSQLException();
	}

	@Override
	public boolean execute(String sql, String[] arg1) throws SQLException {
		throw DbException.get(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT, "execute")
			.getSQLException();
	}

	// TODO 実際にサーバに投げるのをまとめてやるように修正する
	@Override
	public int[] executeBatch() throws SQLException {
		if (batchParameters == null){
			batchParameters = new ArrayList<SolrValue[]>();
		}

		int[] result = new int[batchParameters.size()];
		for (int i=0; i < batchParameters.size(); i++) {
			SolrValue[] set = batchParameters.get(i);
			List<Parameter> parameters = command.getParameters();
			for (int j=0; j < set.length; j++) {
				Parameter param = parameters.get(j);
				param.setValue(set[j]);
			}
			result[i] = executeUpdate();
		}
		return result;
	}

	@Override
	public ResultSet executeQuery(String arg0) throws SQLException {
		throw DbException.get(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT);
	}

	@Override
	public int executeUpdate(String arg0) throws SQLException {
		throw DbException.get(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT);
	}

	@Override
	public int executeUpdate(String arg0, int arg1) throws SQLException {
		throw DbException.get(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT);
	}

	@Override
	public int executeUpdate(String arg0, int[] arg1) throws SQLException {
		throw DbException.get(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT);
	}

	@Override
	public int executeUpdate(String arg0, String[] arg1) throws SQLException {
		throw DbException.get(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT);
	}

	private void setParameter(int parameterIndex, SolrValue value) throws SQLException {
		checkClosed();
		parameterIndex--;
		List<Parameter> parameters = command.getParameters();
		if(parameterIndex < 0 || parameterIndex >= parameters.size()) {
			throw DbException.getInvalidValueException("parameterIndex", String.valueOf(parameterIndex+1));
		}
		Parameter param = parameters.get(parameterIndex);
		param.setValue(value);
	}

}