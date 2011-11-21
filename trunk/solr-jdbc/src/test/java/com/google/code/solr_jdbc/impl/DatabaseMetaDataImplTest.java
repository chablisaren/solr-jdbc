package com.google.code.solr_jdbc.impl;

import java.sql.DriverManager;
import java.sql.SQLException;

import junit.framework.Assert;

import org.junit.Test;

import com.google.code.solr_jdbc.SolrDriver;

public class DatabaseMetaDataImplTest {
	@Test
	public void testGetPattern() throws SQLException, ClassNotFoundException {
		Class.forName(SolrDriver.class.getName());
		SolrConnection conn = SolrConnection.class.cast(DriverManager.getConnection("jdbc:solr:s"));
		DatabaseMetaDataImpl impl = new DatabaseMetaDataImpl(conn);
		Assert.assertTrue(impl.getPattern("AB%").matcher("ABCD").matches());
		Assert.assertTrue(!impl.getPattern("AB\\%").matcher("ABCD").matches());
		Assert.assertTrue(impl.getPattern("AB\\%D").matcher("AB%D").matches());

	}
}
