package solr.jdbc;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.TestCase;

public class SelectQueryTest extends TestCase {
	Connection conn;
	
	@Override
	public void setUp() throws Exception {
		conn = DriverManager.getConnection("jdbc:solr:s");
	}
	
	public void tearDown() throws Exception {
		conn.close();
	}
	
	public void testStatement() {
		String[] expected = {"高橋慶彦","山崎隆造","衣笠祥雄","山本浩二","ランディーバース"};
		try {
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT player_name FROM player ORDER BY player_id");
			int i=0;
			while(rs.next()) {
				assertEquals(expected[i], rs.getString("player_name"));
				i+=1;
			}
		} catch(SQLException e) {
			fail("SQLException:" + e.getMessage());
		}
	}

	public void testStatementLimit() {
		String[] expected = {"山崎隆造","衣笠祥雄","山本浩二"};
		try {
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT player_name FROM player ORDER BY player_id LIMIT 3 OFFSET 1");
			int i=0;
			while(rs.next()) {
				assertEquals(expected[i], rs.getString("player_name"));
				i+=1;
			}
		} catch(SQLException e) {
			fail("SQLException:" + e.getMessage());
		}
	}
	
	public void testStatementOrderBy() {
		String[] expected = {"ランディーバース","山本浩二","衣笠祥雄","山崎隆造","高橋慶彦"};
		try {
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT player_name FROM player ORDER BY player_id DESC");
			int i=0;
			while(rs.next()) {
				assertEquals(expected[i], rs.getString("player_name"));
				i+=1;
			}
		} catch(SQLException e) {
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
