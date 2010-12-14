package com.google.code.solr_jdbc;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.code.solr_jdbc.message.ErrorCode;

import static org.junit.Assert.*;

public class SelectQueryTest {
	Connection conn;

	@Before
	public void setUp() throws Exception {
		conn = DriverManager.getConnection("jdbc:solr:s");
	}

	@After
	public void tearDown() throws Exception {
		conn.close();
	}

	@Test
	public void testStatement() {
		String[][] expected = {{"高橋慶彦"},{"山崎隆造"},{"衣笠祥雄"},{"山本浩二"},{"ランディーバース"}};
		verifyStatement("SELECT player_name FROM player ORDER BY player_id",
				expected);
	}

	@Test
	public void testStatementLimit() {
		String[][] expected = {{"山崎隆造"},{"衣笠祥雄"},{"山本浩二"}};
		verifyStatement("SELECT player_name FROM player ORDER BY player_id LIMIT 3 OFFSET 1",
				expected);
	}

	@Test
	public void testStatementOrderBy() {
		Object[][] expected = {{"ランディーバース"},{"山本浩二"},{"衣笠祥雄"},{"山崎隆造"},{"高橋慶彦"}};
		verifyStatement("SELECT player_name FROM player ORDER BY player_id DESC",
				expected);
	}

	@Test
	public void testStatementCondition() {
		Object[][] expected1 = {{"山崎隆造"}};
		verifyStatement("SELECT player_name FROM player WHERE player_id > 1 AND player_id < 3",
				expected1);
		Object[][] expected2 = {{"高橋慶彦"}, {"山崎隆造"}, {"衣笠祥雄"}};
		verifyStatement("SELECT player_name FROM player WHERE player_id >= 1 AND player_id <= 3",
				expected2);
	}
	
	@Test
	public void testStatementOr() {
		Object[][] expected1 = {{"ランディーバース"}};
		Object[] params = {"阪神"};
		verifyPreparedStatement(
				"SELECT player_name FROM player WHERE (player_id = 1 OR player_id = 5) AND team=?",
				params,
				expected1);
	}

	@Test
	public void testStatementGroupBy() {
		Object[][] expected = {{"カープ", "4"}, {"阪神", "1"}};
		verifyStatement("SELECT team, count(*) FROM player GROUP BY team", expected);
	}
	
	@Test
	public void testStatementIn() {
		Object[][] expected = {{"山崎隆造"}, {"衣笠祥雄"}, {"ランディーバース"}};
		Object[] params = {"一塁手", "二塁手"};
		verifyPreparedStatement(
				"SELECT player_name FROM player WHERE position in (?,?) order by player_id",
				params,
				expected);
	}

	@Test
	public void testStatementTableNotFound() {
		try {
			conn.prepareStatement("select * from prayer");
			fail("No Exception");
		} catch (SQLException e) {
			assertEquals("TableOrViewNotFound", ErrorCode.TABLE_OR_VIEW_NOT_FOUND, e.getErrorCode());
		}

	}

	@Test
	public void testStatementColumnNotFound() {
		try {
			conn.prepareStatement("select prayer_name from player");
			fail("No Exception");
		} catch (SQLException e) {
			assertEquals("ColumnNotFound", ErrorCode.COLUMN_NOT_FOUND, e.getErrorCode());
		}
	}

	/**
	 * get resultSet by column name
	 */
	@Test
	public void testGetColumnLabel() throws SQLException {
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("Select player_name from player where player_id=3");
		assertTrue(rs.next());
		assertEquals("player_name", rs.getMetaData().getColumnLabel(1));
		assertEquals("衣笠祥雄", rs.getString("player_name"));
	}
	
	
	private void verifyStatement(String selectQuery, Object[][] expected) {
		try {
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(selectQuery);
			int i=0;
			while(rs.next()) {
				for(int j=0; j<expected[i].length; j++) {
					assertEquals(expected[i][j], rs.getString(j+1));
				}
				i+=1;
			}
			assertEquals("件数が正しい", expected.length, i);
		} catch(SQLException e) {
			e.printStackTrace();
			fail("SQLException:" + e.getMessage());
		}
	}

	private void verifyPreparedStatement(String selectQuery, Object[] params, Object[][] expected) {
		try {
			PreparedStatement stmt = conn.prepareStatement(selectQuery);
			for(int i=0; i<params.length; i++) {
				stmt.setObject(i+1, params[i]);
			}
			ResultSet rs = stmt.executeQuery();
			int i=0;
			while(rs.next()) {
				for(int j=0; j<expected[i].length; j++) {
					assertEquals(expected[i][j], rs.getString(j+1));
				}
				i+=1;
			}
		} catch(SQLException e) {
			e.printStackTrace();
			fail("SQLException:" + e.getMessage());
		}

	}

	@BeforeClass
	public static void init() throws Exception {
		Connection setUpConn;
		Class.forName(SolrDriver.class.getName());

		setUpConn = DriverManager.getConnection("jdbc:solr:s");
		PreparedStatement dropStmt = setUpConn.prepareStatement("DROP TABLE player");
		try {
			dropStmt.executeUpdate();
		} catch(Exception ignore) {
		}

		PreparedStatement stmt = setUpConn.prepareStatement(
				"CREATE TABLE player (player_id number, team varchar(10), player_name varchar(50), position varchar(10) ARRAY, registered_at DATE)");
		stmt.executeUpdate();

		PreparedStatement insStmt = setUpConn.prepareStatement("INSERT INTO player Values (?,?,?,?,?)");
		insStmt.setInt(1, 1);
		insStmt.setString(2, "カープ");
		insStmt.setString(3, "高橋慶彦");
		insStmt.setObject(4, new String[]{"遊撃手"});
		insStmt.setDate(5, new Date(System.currentTimeMillis()));
		insStmt.executeUpdate();

		insStmt.setInt(1, 2);
		insStmt.setString(2, "カープ");
		insStmt.setString(3, "山崎隆造");
		insStmt.setObject(4, new String[]{"遊撃手","二塁手"});
		insStmt.setDate(5, new Date(System.currentTimeMillis()));
		insStmt.executeUpdate();

		insStmt.setInt(1, 3);
		insStmt.setString(2, "カープ");
		insStmt.setString(3, "衣笠祥雄");
		insStmt.setObject(4, new String[]{"一塁手","三塁手"});
		insStmt.setDate(5, new Date(System.currentTimeMillis()));
		insStmt.executeUpdate();

		insStmt.setInt(1, 4);
		insStmt.setString(2, "カープ");
		insStmt.setString(3, "山本浩二");
		insStmt.setObject(4, new String[]{"外野手"});
		insStmt.setDate(5, new Date(System.currentTimeMillis()));
		insStmt.executeUpdate();

		insStmt.setInt(1, 5);
		insStmt.setString(2, "阪神");
		insStmt.setString(3, "ランディーバース");
		insStmt.setObject(4, new String[]{"一塁手","外野手"});
		insStmt.setDate(5, new Date(System.currentTimeMillis()));
		insStmt.executeUpdate();

		setUpConn.commit();
	}
}
