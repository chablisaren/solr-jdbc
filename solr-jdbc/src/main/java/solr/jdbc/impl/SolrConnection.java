package solr.jdbc.impl;

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

import solr.jdbc.message.DbException;
import solr.jdbc.message.ErrorCode;



public abstract class SolrConnection implements Connection {
	private SolrServer solrServer;
	private DatabaseMetaDataImpl metaData;
	private final Boolean isClosed = false;

	private final int holdability = 1;
	private boolean autoCommit = false;


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
		// TODO Auto-generated method stub

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
	public String getCatalog() throws SQLException {
		checkClosed();
		return null;
	}

	@Override
	public Properties getClientInfo() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getClientInfo(String arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getHoldability() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
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
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isClosed() throws SQLException {
		return isClosed;
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		// TODO Auto-generated method stub
		return false;
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
	public CallableStatement prepareCall(String arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CallableStatement prepareCall(String arg0, int arg1, int arg2)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CallableStatement prepareCall(String arg0, int arg1, int arg2,
			int arg3) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		checkClosed();
		PreparedStatementImpl stmt = new PreparedStatementImpl(this, sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		return stmt;
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int arg1)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PreparedStatement prepareStatement(String arg0, int[] arg1)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PreparedStatement prepareStatement(String arg0, String[] arg1)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PreparedStatement prepareStatement(String arg0, int arg1, int arg2)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PreparedStatement prepareStatement(String arg0, int arg1, int arg2,
			int arg3) throws SQLException {
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
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		this.autoCommit  = autoCommit;
	}

	@Override
	public void setCatalog(String catalog) throws SQLException {
		checkClosed();
		// ignore
	}

	@Override
	public void setClientInfo(Properties arg0) throws SQLClientInfoException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setClientInfo(String arg0, String arg1)
			throws SQLClientInfoException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setHoldability(int arg0) throws SQLException {
		// TODO Auto-generated method stub

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
	public void setTransactionIsolation(int arg0) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setTypeMap(Map<String, Class<?>> arg0) throws SQLException {
		// TODO Auto-generated method stub

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

	protected void checkClosed() throws SQLException {
		if (isClosed) {
			throw new SQLException("Already closed.");
		}
	}
}
