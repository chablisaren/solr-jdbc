package com.google.code.solr_jdbc.impl;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;

import com.google.code.solr_jdbc.SolrColumn;
import com.google.code.solr_jdbc.message.DbException;
import com.google.code.solr_jdbc.message.ErrorCode;
import com.google.code.solr_jdbc.value.DataType;




public class DatabaseMetaDataImpl implements DatabaseMetaData {
	private final SolrConnection conn;
	private Map<String, List<SolrColumn>> tableColumns;

	public DatabaseMetaDataImpl(SolrConnection conn) {
		this.conn = conn;
		buildMetadata();
	}

	private void buildMetadata() {
		this.tableColumns = new HashMap<String, List<SolrColumn>>();
		try {
			QueryResponse res = this.conn.getSolrServer().query(
					new SolrQuery("meta.name:[A TO z]"));

			for (SolrDocument doc : res.getResults()) {
				String tableName = doc.getFieldValue("meta.name").toString();
				List<SolrColumn> columns = new ArrayList<SolrColumn>();
				for (Object cols : doc.getFieldValues("meta.columns")) {
					columns.add(new DefaultSolrColumn(tableName + "." + cols.toString()));
				}
				tableColumns.put(tableName, columns);
			}

		} catch (Exception e) {
			throw DbException.get(ErrorCode.IO_EXCEPTION, e);
		}
	}

	public SolrColumn getSolrColumn(String tableName, String columnName) {
		for(SolrColumn solrColumn : this.tableColumns.get(tableName)) {
			if (StringUtils.equals(solrColumn.getColumnName(), columnName)) {
				return solrColumn;
			}
		}
		return null;

	}
	public List<SolrColumn> getSolrColumns(String tableName) {
		if(tableColumns == null)
			buildMetadata();
		return this.tableColumns.get(tableName);
	}
	@Override
	public boolean allProceduresAreCallable() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean allTablesAreSelectable() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean deletesAreDetected(int i) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public ResultSet getAttributes(String s, String s1, String s2, String s3)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getBestRowIdentifier(String s, String s1, String s2,
			int i, boolean flag) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getBestRowIdentifier");
	}

	@Override
	public String getCatalogSeparator() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getCatalogSeparator");
	}

	@Override
	public String getCatalogTerm() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getCatalogTerm");
	}

	@Override
	public ResultSet getCatalogs() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getCatalogs");
	}

	@Override
	public ResultSet getClientInfoProperties() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getClientInfoProperties");
	}

	@Override
	public ResultSet getColumnPrivileges(String s, String s1, String s2,
			String s3) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getColumnPrivileges");
	}

	@Override
	public ResultSet getColumns(String catalog, String schema, String table,
			String columnNamePattern) throws SQLException {
		if (tableColumns == null) {
			buildMetadata();
		}

		CollectionResultSet rs;
		String[] columns = { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME",
				"DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS",
				"NUM_PREC_RADIX", "NULLABLE", "REMARKS", "COLUMN_DEF",
				"SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION",
				"IS_NULLABLE", "SCOPE_CATLOG", "SCOPE_SCHEMA", "SCOPE_TABLE", "SOURCE_DATA_TYPE"
		};
		rs = new CollectionResultSet();
		rs.setColumns(Arrays.asList(columns));

		for (SolrColumn column : tableColumns.get(table)) {
			Object[] columnMeta = new Object[22];
			columnMeta[1] = ""; // TABLE_SCHEM
			columnMeta[2] = column.getTableName();
			columnMeta[3] = column.getColumnName(); // COLUMN_NAME

			columnMeta[4] = DataType.getDataType(column.getType()).sqlType; // DATA_TYPE
			columnMeta[5] = DataType.getDataType(column.getType()).jdbc; // TYPE_NAME
			columnMeta[6] = 0; // COLUMN_SIZE
			columnMeta[8] = 0; // DECIMAL_DIGITS
			columnMeta[10] = DatabaseMetaData.columnNullableUnknown; // NULLABLE
			rs.add(Arrays.asList(columnMeta));
		}

		return rs;
	}

	@Override
	public Connection getConnection() throws SQLException {
		return conn;
	}

	@Override
	public ResultSet getCrossReference(String s, String s1, String s2,
			String s3, String s4, String s5) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getCrossReference");
	}

	@Override
	public String getDatabaseProductName() throws SQLException {
		return "solr";
	}

	@Override
	public int getDatabaseMajorVersion() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getDatabaseMajorVersion");
	}

	@Override
	public int getDatabaseMinorVersion() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getDatabaseMinorVersion");
	}


	@Override
	public String getDatabaseProductVersion() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getDatabaseProductVersion");
	}

	@Override
	public int getDefaultTransactionIsolation() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getDriverMajorVersion() {
		return 0;
	}

	@Override
	public int getDriverMinorVersion() {
		return 1;
	}

	@Override
	public String getDriverName() throws SQLException {
		return "solr-jdbc";
	}

	@Override
	public String getDriverVersion() throws SQLException {
		return "0.1.0";
	}

	@Override
	public ResultSet getExportedKeys(String s, String s1, String s2)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getExportedKeys");
	}

	@Override
	public String getExtraNameCharacters() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getExtraNameCharacters");
	}

	@Override
	public ResultSet getFunctionColumns(String s, String s1, String s2,
			String s3) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getFunctionColumns");
	}

	@Override
	public ResultSet getFunctions(String s, String s1, String s2)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getFunctions");
	}

	@Override
	public String getIdentifierQuoteString() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getFunctionColumns");
	}

	@Override
	public ResultSet getImportedKeys(String catalog, String schema, String table)
			throws SQLException {
		CollectionResultSet rs = new CollectionResultSet();
		String[] columns = {
				"PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME",
				"FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME",
				"KEY_SEQ", "UPDATE_RULE", "DELETE_RULE", "FK_NAME", "PK_NAME", "DEFERRABILITY"
				};
		rs.setColumns(Arrays.asList(columns));
		return rs;
	}

	@Override
	public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique,
			boolean approximate) throws SQLException {
		CollectionResultSet rs = new CollectionResultSet();
		String[] columns = {
				"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "NON_UNIQUE",
				"INDEX_QUALIFIER", "INDEX_NAME", "TYPE", "ORDINAL_POSITION",
				"COLUMN_NAME", "ASC_OR_DESC", "CARDINALITY", "PAGES", "FILTER_CONDITION"
				};
		rs.setColumns(Arrays.asList(columns));
		return rs;
	}

	@Override
	public int getJDBCMajorVersion() throws SQLException {
		return 2;
	}

	@Override
	public int getJDBCMinorVersion() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxBinaryLiteralLength() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getMaxBinaryLiteralLength");
	}

	@Override
	public int getMaxCatalogNameLength() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getMaxCatalogNameLength");
	}

	@Override
	public int getMaxCharLiteralLength() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getMaxCharLiteralLength");
	}

	@Override
	public int getMaxColumnNameLength() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getMaxColumnNameLength");
	}

	/**
	 * SolrのFacetの仕様のため、1を返す.
	 * 
	 */
	@Override
	public int getMaxColumnsInGroupBy() throws SQLException {
		return 1;
	}

	@Override
	public int getMaxColumnsInIndex() throws SQLException {
		return 255;
	}

	@Override
	public int getMaxColumnsInOrderBy() throws SQLException {
		return 255;
	}

	@Override
	public int getMaxColumnsInSelect() throws SQLException {
		return 255;
	}

	@Override
	public int getMaxColumnsInTable() throws SQLException {
		return 255;
	}

	@Override
	public int getMaxConnections() throws SQLException {
		return 255;
	}

	@Override
	public int getMaxCursorNameLength() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getMaxCursorNameLength");
	}

	@Override
	public int getMaxIndexLength() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getMaxIndexLength");
	}

	@Override
	public int getMaxProcedureNameLength() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getMaxProcedureNameLength");
	}

	@Override
	public int getMaxRowSize() throws SQLException {
		return 0; // No limitation
	}

	@Override
	public int getMaxSchemaNameLength() throws SQLException {
		return 0; // No limitation
	}

	@Override
	public int getMaxStatementLength() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxStatements() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxTableNameLength() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxTablesInSelect() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxUserNameLength() throws SQLException {
		return 0; // No limitation
	}

	@Override
	public String getNumericFunctions() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getNumericFunctions");
	}

	@Override
	public ResultSet getPrimaryKeys(String catalog, String schema, String table)
			throws SQLException {
		CollectionResultSet rs;
		String[] columns = { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME",
				"KEY_SEQ", "PK_NAME"
		};
		rs = new CollectionResultSet();
		rs.setColumns(Arrays.asList(columns));

		/* TODO PKをセットする
		for (SolrColumn column : tableColumns.get(table)) {
			Object[] columnMeta = new Object[6];
			columnMeta[2] = column.getTableName();
			columnMeta[3] = column.getColumnName(); // COLUMN_NAME

			columnMeta[4] = DataType.getDataType(column.getType()).sqlType; // KEY_SEQ
			columnMeta[5] = DataType.getDataType(column.getType()).jdbc; // PK_NAME
			rs.add(Arrays.asList(columnMeta));
		}
		*/
		return rs;
	}

	@Override
	public ResultSet getProcedureColumns(String s, String s1, String s2,
			String s3) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getProcedureColumns");
	}

	@Override
	public String getProcedureTerm() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getProcedureTerm");
	}

	@Override
	public ResultSet getProcedures(String s, String s1, String s2)
		throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getProcedureTerm");
	}

	@Override
	public int getResultSetHoldability() throws SQLException {
		return ResultSet.HOLD_CURSORS_OVER_COMMIT;
	}

	@Override
	public RowIdLifetime getRowIdLifetime() throws SQLException {
		return RowIdLifetime.ROWID_UNSUPPORTED;
	}

	@Override
	public String getSQLKeywords() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getSQLKeywords");
	}

	@Override
	public int getSQLStateType() throws SQLException {
		return sqlStateXOpen;
	}

	@Override
	public String getSchemaTerm() throws SQLException {
		return "schema";
	}

	@Override
	public ResultSet getSchemas() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getSchemas");
	}

	@Override
	public ResultSet getSchemas(String s, String s1) throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getSchemas");
	}

	@Override
	public String getSearchStringEscape() throws SQLException {
		return "%";
	}

	@Override
	public String getStringFunctions() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getStringFunctions");
	}

	@Override
	public ResultSet getSuperTables(String s, String s1, String s2)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getSuperTables");
	}

	@Override
	public ResultSet getSuperTypes(String s, String s1, String s2)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getSuperTypes");
	}

	@Override
	public String getSystemFunctions() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getSystemFunctions");
	}

	@Override
	public ResultSet getTablePrivileges(String s, String s1, String s2)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getTablePrivileges");
	}

	@Override
	public ResultSet getTableTypes() throws SQLException {
		String[] columns = {"TABLE_TYPE"};
		CollectionResultSet rs = new CollectionResultSet();
		rs.setColumns(Arrays.asList(columns));
		Object[] record = {"TABLE"}; 
		rs.add(Arrays.asList(record));
		return rs;
	}

	@Override
	public ResultSet getTables(String catalog, String schema,
			String tableNamePattern, String[] types) throws SQLException {
		if (tableColumns == null) {
			buildMetadata();
		}

		CollectionResultSet rs;
		String[] columns = { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME",
				"TABLE_TYPE", "REMARKS", "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME",
				"SELF_REFERENCING_COL_NAME", "REF_GENERATION" };
		rs = new CollectionResultSet();
		rs.setColumns(Arrays.asList(columns));

		for (String tableName : tableColumns.keySet()) {
			Object[] tableMeta = { null, "", tableName, "TABLE", "", null,
					null, null, null, null };
			rs.add(Arrays.asList(tableMeta));
		}

		return rs;
	}

	@Override
	public String getTimeDateFunctions() throws SQLException {
		return "";
	}

	@Override
	public ResultSet getTypeInfo() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getTypeInfo");
	}

	@Override
	public ResultSet getUDTs(String s, String s1, String s2, int[] ai)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getUDTs");
	}

	@Override
	public String getURL() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getURL");
	}

	@Override
	public String getUserName() throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getUserName");
	}

	@Override
	public ResultSet getVersionColumns(String s, String s1, String s2)
			throws SQLException {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "getVersionColumns");
	}

	@Override
	public boolean insertsAreDetected(int i) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isCatalogAtStart() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean locatorsUpdateCopy() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean nullPlusNonNullIsNull() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean nullsAreSortedAtEnd() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean nullsAreSortedAtStart() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean nullsAreSortedHigh() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean nullsAreSortedLow() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean othersDeletesAreVisible(int i) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean othersInsertsAreVisible(int i) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean othersUpdatesAreVisible(int i) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean ownDeletesAreVisible(int i) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean ownInsertsAreVisible(int i) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean ownUpdatesAreVisible(int i) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean storesLowerCaseIdentifiers() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean storesMixedCaseIdentifiers() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean storesUpperCaseIdentifiers() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsANSI92EntryLevelSQL() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsANSI92FullSQL() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsANSI92IntermediateSQL() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsAlterTableWithAddColumn() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsAlterTableWithDropColumn() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsBatchUpdates() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsCatalogsInDataManipulation() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsCatalogsInProcedureCalls() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsCatalogsInTableDefinitions() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsColumnAliasing() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsConvert() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsConvert(int i, int j) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsCoreSQLGrammar() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsCorrelatedSubqueries() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsDataDefinitionAndDataManipulationTransactions()
			throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsDataManipulationTransactionsOnly()
			throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsDifferentTableCorrelationNames() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsExpressionsInOrderBy() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsExtendedSQLGrammar() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsFullOuterJoins() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsGetGeneratedKeys() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsGroupBy() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsGroupByBeyondSelect() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsGroupByUnrelated() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsIntegrityEnhancementFacility() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsLikeEscapeClause() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsLimitedOuterJoins() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsMinimumSQLGrammar() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsMixedCaseIdentifiers() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsMultipleOpenResults() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsMultipleResultSets() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsMultipleTransactions() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsNamedParameters() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsNonNullableColumns() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsOrderByUnrelated() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsOuterJoins() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsPositionedDelete() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsPositionedUpdate() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsResultSetConcurrency(int i, int j)
			throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsResultSetHoldability(int i) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsResultSetType(int i) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsSavepoints() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsSchemasInDataManipulation() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsSchemasInIndexDefinitions() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsSchemasInProcedureCalls() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsSchemasInTableDefinitions() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsSelectForUpdate() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsStatementPooling() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsStoredProcedures() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsSubqueriesInComparisons() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsSubqueriesInExists() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsSubqueriesInIns() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsSubqueriesInQuantifieds() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsTableCorrelationNames() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsTransactionIsolationLevel(int i) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsTransactions() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsUnion() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsUnionAll() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean updatesAreDetected(int i) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean usesLocalFilePerTable() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean usesLocalFiles() throws SQLException {
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
}
