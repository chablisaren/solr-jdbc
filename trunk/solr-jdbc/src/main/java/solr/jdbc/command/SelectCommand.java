package solr.jdbc.command;

import java.sql.ResultSet;
import java.util.Map;

import net.sf.jsqlparser.statement.select.Select;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;

import solr.jdbc.SolrSelectVisitor;
import solr.jdbc.impl.DatabaseMetaDataImpl;
import solr.jdbc.impl.DefaultResultSetImpl;
import solr.jdbc.impl.FacetResultSetImpl;
import solr.jdbc.message.DbException;
import solr.jdbc.message.ErrorCode;
import solr.jdbc.value.SolrValue;

public class SelectCommand extends Command {
	private final Select select;
	private SolrSelectVisitor selectVisitor;

	public SelectCommand(Select statement) {
		this.select = statement;
	}

	@Override
	public boolean isQuery() {
		return true;
	}
	
	@Override
	public void parse() {
		DatabaseMetaDataImpl metaData= this.conn.getMetaDataImpl();
		selectVisitor = new SolrSelectVisitor(metaData);
		select.getSelectBody().accept(selectVisitor);
		initParameters(selectVisitor.getParameterSize());
	}

	@Override
	public ResultSet executeQuery() {
		SolrQuery query = new SolrQuery(selectVisitor.getQuery(parameters.toArray(new SolrValue[0])));
		Map<String,String> options = selectVisitor.getSolrOptions();
		for(Map.Entry<String, String> entry:options.entrySet()) {
			query.set(entry.getKey(), entry.getValue());
		}
		ResultSet rs = null;
		QueryResponse response;
		try {
			response = conn.getSolrServer().query(query);
			if(selectVisitor.hasGroupBy()) {
				rs = new FacetResultSetImpl(response, selectVisitor.getResultSetMetaData());
			} else {
				rs = new DefaultResultSetImpl(response.getResults(), selectVisitor.getResultSetMetaData());
			}
		} catch (SolrServerException e) {
			DbException.get(ErrorCode.GENERAL_ERROR, e);
		}

		return rs;
	}

	@Override
	public int executeUpdate() {
		throw DbException.get(ErrorCode.METHOD_NOT_ALLOWED_FOR_QUERY);
	}

}
