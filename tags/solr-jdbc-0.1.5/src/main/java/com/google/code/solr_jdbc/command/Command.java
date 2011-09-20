package com.google.code.solr_jdbc.command;

import java.util.List;

import com.google.code.solr_jdbc.expression.Parameter;
import com.google.code.solr_jdbc.impl.AbstractResultSet;
import com.google.code.solr_jdbc.impl.SolrConnection;


public abstract class Command {
	protected SolrConnection conn;
	protected List<Parameter> parameters;

	public abstract AbstractResultSet executeQuery();
	public abstract int executeUpdate();

	public void setConnection(SolrConnection conn) {
		this.conn = conn;
	}
	public List<Parameter> getParameters() {
		return parameters;
	}

	public abstract void parse();
	public abstract boolean isQuery();
	
}
