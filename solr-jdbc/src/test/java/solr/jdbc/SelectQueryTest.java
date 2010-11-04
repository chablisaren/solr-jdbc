package solr.jdbc;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import solr.jdbc.message.ErrorCode;

import junit.framework.TestCase;

public class SelectQueryTest extends TestCase {
	Connection conn;
	
	@Override
	public void setUp() throws Exception {
		conn = DriverManager.getConnection("jdbc:solr:s");
	}
	
	@Override
	public void tearDown() throws Exception {
		conn.close();
	}
	
	public void testStatement() {
		String[][] expected = {{"高橋慶彦"},{"山崎隆造"},{"衣笠祥雄"},{"山本浩二"},{"ランディーバース"}};
		verifyStatement("SELECT player_name FROM player ORDER BY player_id",
				expected);
	}

	public void testStatementLimit() {
		String[][] expected = {{"山崎隆造"},{"衣笠祥雄"},{"山本浩二"}};
		verifyStatement("SELECT player_name FROM player ORDER BY player_id LIMIT 3 OFFSET 1",
				expected);
	}
	
	public void testStatementOrderBy() {
		Object[][] expected = {{"ランディーバース"},{"山本浩二"},{"衣笠祥雄"},{"山崎隆造"},{"高橋慶彦"}};
		verifyStatement("SELECT player_name FROM player ORDER BY player_id DESC",
				expected);
	}

	public void testStatementCondition() {
		Object[][] expected1 = {{"山崎隆造"}};
		verifyStatement("SELECT player_name FROM player WHERE player_id > 1 AND player_id < 3",
				expected1);
		Object[][] expected2 = {{"高橋慶彦"}, {"山崎隆造"}, {"衣笠祥雄"}};
		verifyStatement("SELECT player_name FROM player WHERE player_id >= 1 AND player_id <= 3",
				expected2);
	}
	public void testStatementOr() {
		Object[][] expected1 = {{"ランディーバース"}};
		Object[] params = {"阪神"};
		verifyPreparedStatement(
				"SELECT player_name FROM player WHERE (player_id = 1 OR player_id = 5) AND team=?",
				params,
				expected1);
	}
	
	public void testStatementGroupBy() {
		Object[][] expected = {{"カープ", "4"}, {"阪神", "1"}};
		verifyStatement("SELECT team, count(*) FROM player GROUP BY team", expected);
	}

	public void testStatementTableNotFound() {
		try {
			conn.prepareStatement("select * from prayer");
			fail("No Exception");
		} catch (SQLException e) {
			assertEquals("TableOrViewNotFound", ErrorCode.TABLE_OR_VIEW_NOT_FOUND, e.getErrorCode());
		}
		
	}
	
	public void testStatementColumnNotFound() {
		try {
			conn.prepareStatement("select prayer_name from player");
			fail("No Exception");
		} catch (SQLException e) {
			assertEquals("ColumnNotFound", ErrorCode.COLUMN_NOT_FOUND, e.getErrorCode());
		}
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
	
	public SelectQueryTest() throws Exception {
		super();
		Connection setUpConn;
		Class.forName("solr.jdbc.SolrDriver");

		setUpConn = DriverManager.getConnection("jdbc:solr:s");
		PreparedStatement dropStmt = setUpConn.prepareStatement("DROP TABLE player");
		try {
			dropStmt.executeUpdate();
		} catch(Exception ignore) {
		}

		PreparedStatement stmt = setUpConn.prepareStatement(
				"CREATE TABLE player (player_id number, team varchar(10), player_name varchar(50), registered_at DATE)");
		stmt.executeUpdate();

		PreparedStatement insStmt = setUpConn.prepareStatement("INSERT INTO player Values (?,?,?,?)");
		insStmt.setInt(1, 1);
		insStmt.setString(2, "カープ");
		insStmt.setString(3, "高橋慶彦");
		insStmt.setDate(4, new Date(System.currentTimeMillis()));
		insStmt.executeUpdate();

		insStmt.setInt(1, 2);
		insStmt.setString(2, "カープ");
		insStmt.setString(3, "山崎隆造");
		insStmt.setDate(4, new Date(System.currentTimeMillis()));
		insStmt.executeUpdate();
		insStmt.setInt(1, 3);
		insStmt.setString(2, "カープ");
		insStmt.setString(3, "衣笠祥雄");
		insStmt.setDate(4, new Date(System.currentTimeMillis()));
		insStmt.executeUpdate();

		insStmt.setInt(1, 4);
		insStmt.setString(2, "カープ");
		insStmt.setString(3, "山本浩二");
		insStmt.setDate(4, new Date(System.currentTimeMillis()));
		insStmt.executeUpdate();

		insStmt.setInt(1, 5);
		insStmt.setString(2, "阪神");
		insStmt.setString(3, "ランディーバース");
		insStmt.setDate(4, new Date(System.currentTimeMillis()));
		insStmt.executeUpdate();

		setUpConn.commit();
	}
}
