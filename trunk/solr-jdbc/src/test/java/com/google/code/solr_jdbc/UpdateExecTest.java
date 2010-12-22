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

import static org.junit.Assert.*;


public class UpdateExecTest {
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
	public void update() throws SQLException {
		Statement stmt = conn.createStatement();
		try {
			
		} finally {
			stmt.close();
		}
	}
	
	@Test
	public void delete() throws SQLException {
		Statement stmt = conn.createStatement();
		try {
			ResultSet rs1 = stmt.executeQuery("SELECT count(*) AS cnt FROM player WHERE player_id=5");
			rs1.next();
			assertEquals("バースがいる", 1, rs1.getInt("cnt"));
			stmt.executeUpdate("DELETE FROM player WHERE player_id = 5");
			conn.commit();
			ResultSet rs2 = stmt.executeQuery("SELECT count(*) AS cnt FROM player WHERE player_id=5");
			rs2.next();
			assertEquals("バースが消える", 0, rs2.getInt("cnt"));
		} finally {
			stmt.close();

			// 元に戻しておく
			PreparedStatement ps = conn.prepareStatement("INSERT INTO  player Values (?,?,?,?,?)");
			try {
				ps.setInt(1, 5);
				ps.setString(2, "阪神");
				ps.setString(3, "ランディーバース");
				ps.setObject(4, new String[]{"一塁手","外野手"});
				ps.setDate(5, new Date(System.currentTimeMillis()));
				ps.executeUpdate();
				conn.commit();
			} finally {
				ps.close();
			}
		}
	}
	
	@BeforeClass
	public static void init() throws SQLException, ClassNotFoundException {
		Connection setUpConn = null;
		Class.forName(SolrDriver.class.getName());

		try {
			setUpConn = DriverManager.getConnection("jdbc:solr:s");
			PreparedStatement dropStmt = setUpConn.prepareStatement("DROP TABLE player");
			try {
				dropStmt.executeUpdate();
			} catch(SQLException ignore) {
			} finally {
				dropStmt.close();
			}
	
			PreparedStatement stmt = setUpConn.prepareStatement(
					"CREATE TABLE player (player_id number, team varchar(10), player_name varchar(50), position varchar(10) ARRAY, registered_at DATE)");
			try {
				stmt.executeUpdate();
			} finally {
				stmt.close();
			}
	
			PreparedStatement insStmt = setUpConn.prepareStatement("INSERT INTO player Values (?,?,?,?,?)");
			try {
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
			} finally {
				insStmt.close();
			}
	
			setUpConn.commit();
		} finally {
			setUpConn.close();
		}
	}

}
