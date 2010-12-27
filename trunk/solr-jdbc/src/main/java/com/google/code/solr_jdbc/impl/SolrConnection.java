package com.google.code.solr_jdbc.impl;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;

import org.apache.solr.client.solrj.SolrServer;

import com.google.code.solr_jdbc.message.DbException;
import com.google.code.solr_jdbc.message.ErrorCode;




public abstract class SolrConnection implements Connection {
	private SolrServer solrServer;
	private DatabaseMetaDataImpl metaData;
	private final Boolean isClosed = false;

	private int holdability = ResultSet.HOLD_CURSORS_OVER_COMMIT;
	private boolean autoCommit = false;
	private String catalog;
	private Statement executingStatement;

	protected SolrConnection(String serverUrl) {

	}

	protected void setSolrServer(SolrServer solrServer){
		this.solrServer = solrServer;
	}

	public SolrServer getSolrServer() {
		return solrServer;
	}
	@Override
	public void clearWarnings() throws SQLException {
		checkClosed();
	}

	@Override
	public void commit() throws SQLException {
		try {
			solrServer.commit();
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

	@Override
	public Array createArrayOf(String arg0, Object[] arg1) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "createArrayOf");
	}

	@Override
	public Blob createBlob() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "createBlob");
	}

	@Override
	public Clob createClob() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "createClob");
	}

	@Override
	public NClob createNClob() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "createNClob");
	}

	@Override
	public SQLXML createSQLXML() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "createSQLXML");
	}

	@Override
	public Struct createStruct(String arg0, Object[] arg1) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "createStruct");
	}

	@Override
	public Statement createStatement() throws SQLException {
		checkClosed();
		return new StatementImpl(this, ResultSet.FETCH_FORWARD, ResultSet.CONCUR_READ_ONLY);
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
		checkClosed();
		return new StatementImpl(this, resultSetType, resultSetConcurrency);
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		checkClosed();
		return new StatementImpl(this, resultSetType, resultSetConcurrency);
	}

	@Override
	public boolean getAutoCommit() throws SQLException {
		checkClosed();
		return autoCommit;
	}

	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		checkClosed();
		this.autoCommit  = autoCommit;
	}

	@Override
	public String getCatalog() throws SQLException {
		checkClosed();
		return catalog;
	}

	@Override
	public void setCatalog(String catalog) throws SQLException {
		checkClosed();
		// ignore
	}

	@Override
	public Properties getClientInfo() throws SQLException {
		throw new SQLClientInfoException();
	}

	@Override
	public String getClientInfo(String name) throws SQLException {
		throw new SQLClientInfoException();
	}

	@Override
	public void setClientInfo(Properties properties) throws SQLClientInfoException {
		throw new SQLClientInfoException();
	}

	@Override
	public void setClientInfo(String arg0, String arg1)
			throws SQLClientInfoException {
		throw new SQLClientInfoException();
	}

	@Override
	public int getHoldability() throws SQLException {
		checkClosed();
		return holdability;
	}

	@Override
	public void setHoldability(int holdability) throws SQLException {
		checkClosed();
		this.holdability = holdability;
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
		if (metaData == null) {
			metaData = new DatabaseMetaDataImpl(this);
		}
		return metaData;
	}
	
	public DatabaseMetaDataImpl getMetaDataImpl() {
		if (metaData == null) {
			metaData = new DatabaseMetaDataImpl(this);
		}
		return metaData;
	}

	@Override
	public int getTransactionIsolation() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void setTransactionIsolation(int arg0) throws SQLException {
		// TODO Auto-generated method stub
	}


	@Override
	public boolean isReadOnly() throws SQLException {
		checkClosed();
		return false;
	}

	@Override
	public void setReadOnly(boolean arg0) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public Savepoint setSavepoint() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Savepoint setSavepoint(String arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		checkClosed();
		return null;
	}

	@Override
	public void setTypeMap(Map<String, Class<?>> arg0) throws SQLException {
		// do nothing
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		checkClosed();
		return null;
	}

	@Override
	public boolean isClosed() throws SQLException {
		return isClosed;
	}

	@Override
	public boolean isValid(int arg0) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String nativeSQL(String arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CallableStatement prepareCall(String sql) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "prepareCall");
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "prepareCall");
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "prepareCall");
	}

	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		checkClosed();
		PreparedStatementImpl stmt;
		try {
			stmt = new PreparedStatementImpl(this, sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		} catch(DbException e) {
			throw e.getSQLException();
		}
		return stmt;
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKey)
			throws SQLException {
		return prepareStatement(sql);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
			throws SQLException {
		return prepareStatement(sql);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames)
			throws SQLException {
		return prepareStatement(sql);
	}

	@Override
	public PreparedStatement prepareStatement(String arg0, int arg1, int arg2)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PreparedStatement prepareStatement(String arg0, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void releaseSavepoint(Savepoint arg0) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void rollback() throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void rollback(Savepoint arg0) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean isWrapperFor(Class<?> arg0) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public <T> T unwrap(Class<T> arg0) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "unwrap");
	}

	protected void checkClosed() throws SQLException {
		if (isClosed) {
			throw new SQLException("Already closed.");
		}
	}
}
