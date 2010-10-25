package solr.jdbc.command;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import solr.jdbc.impl.SolrConnection;
import solr.jdbc.value.SolrValue;

public abstract class Command {
	protected SolrConnection conn;
	protected List<SolrValue> parameters;

	public abstract ResultSet executeQuery();
	public abstract int executeUpdate();

	public void setConnection(SolrConnection conn) {
		this.conn = conn;
	}
	public List<SolrValue> getParameters() {
		return parameters;
	}

	public abstract void parse();
	protected void initParameters(int size) {
		parameters = new ArrayList<SolrValue>(size);
		for(int i=0; i<size; i++) {
			parameters.add(null);
		}
	}

}
