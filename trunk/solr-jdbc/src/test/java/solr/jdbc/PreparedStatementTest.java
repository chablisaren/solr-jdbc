package solr.jdbc;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import junit.framework.TestCase;



public class PreparedStatementTest extends TestCase{
	Connection conn;

	@Override
	public void setUp() throws Exception{
		Class.forName("solr.jdbc.SolrDriver");

		conn = DriverManager.getConnection("jdbc:solr:s");
		PreparedStatement dropStmt = conn.prepareStatement("DROP TABLE player");
		try {
			dropStmt.executeUpdate();
		} catch(Exception ignore) {
		}

		PreparedStatement stmt = conn.prepareStatement(
				"CREATE TABLE player (player_id number, team varchar(10), player_name varchar(50), registered_at DATE)");
		stmt.executeUpdate();

		PreparedStatement insStmt = conn.prepareStatement("INSERT INTO player Values (?,?,?,?)");
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

		conn.commit();
	}

	@Override
	public void tearDown() throws Exception {
		PreparedStatement dropStmt = conn.prepareStatement("DROP TABLE player");
		dropStmt.executeUpdate();
		conn.close();
	}

	public void test_ノーマルSELECT文() throws Exception {
		assertNotNull(conn);
		PreparedStatement selStmt = conn.prepareStatement(
				"SELECT * FROM player LIMIT 2 OFFSET 0");
//		selStmt.setString(1, "001");
		ResultSet rs = selStmt.executeQuery();
		while(rs.next()) {
//			assertEquals("テスト", rs.getString("product_name"));
			System.out.println(rs.getString("player_name"));
			System.out.println(rs.getString("registered_at"));
		}
	}

	public void test_カラムを選択する() throws Exception {
		PreparedStatement stmt = conn.prepareStatement(
				"SELECT player_name FROM player");
	}

	public void test_グルーピング() throws Exception {
		PreparedStatement selStmt = conn.prepareStatement(
				"SELECT team, count(*) FROM player group by team");
		ResultSet rs = selStmt.executeQuery();
		while(rs.next()) {
			System.out.println(rs.getString("team"));
		}
	}

	public void test_UPDATE() throws SQLException {
		PreparedStatement stmt = conn.prepareStatement(
		"SELECT player_name, team FROM player");
		ResultSet rs = stmt.executeQuery();
		while(rs.next()) {
			System.out.println("player_name="+rs.getString("player_name")+",team="+rs.getString("team"));
		}
		PreparedStatement updStmt = conn.prepareStatement(
				"UPDATE player SET team=? WHERE player_name=?");
		updStmt.setString(1, "カープ");
		updStmt.setString(2, "ランディーバース");
		assertEquals("1件が更新される", 1, updStmt.executeUpdate());
	}
}
