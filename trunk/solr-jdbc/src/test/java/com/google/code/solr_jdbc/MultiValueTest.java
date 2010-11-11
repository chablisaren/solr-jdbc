package com.google.code.solr_jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.junit.Before;
import org.junit.Test;

public class MultiValueTest {
	Connection conn;

	@Before
	public void setUp() throws Exception{
		Class.forName(SolrDriver.class.getName());

		conn = DriverManager.getConnection("jdbc:solr:s");
		PreparedStatement dropStmt = conn.prepareStatement("DROP TABLE books");
		try {
			dropStmt.executeUpdate();
		} catch(Exception ignore) {
		}

		PreparedStatement stmt = conn.prepareStatement(
				"CREATE TABLE books (title varchar(50), author varchar(50) ARRAY )");
		stmt.executeUpdate();

		PreparedStatement insStmt = conn.prepareStatement("INSERT INTO books Values (?,?)");
		insStmt.setString(1, "バカの壁");
		insStmt.setObject(2, new String[]{"養老孟司"});
		insStmt.executeUpdate();

		insStmt.setString(1, "勝間さん、努力で幸せになれますか");
		insStmt.setObject(2, new String[]{"勝間和代", "香山リカ"});
		insStmt.executeUpdate();

		conn.commit();
	}

	@Test
	public void test_multiValue() throws Exception {
		PreparedStatement selStmt = conn.prepareStatement(
				"SELECT * FROM books");
		ResultSet rs = selStmt.executeQuery();
		while(rs.next()) {
			System.out.println(rs.getString("title"));
			System.out.println(rs.getString("author"));
		}
	}

	@Test
	public void test_multiValueWhereClause() throws Exception {
		PreparedStatement selStmt = conn.prepareStatement(
				"SELECT * FROM books where author = ?");
		selStmt.setString(1, "勝間和代");
		ResultSet rs = selStmt.executeQuery();
		while(rs.next()) {
			System.out.println(rs.getString("title"));
			System.out.println(rs.getString("author"));
			Object[] authors = (Object[])rs.getArray("author").getArray();
			for(Object author : authors) {
				System.out.println(author);
			}
		}
	}

}
