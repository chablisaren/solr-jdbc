package solr.jdbc.impl;

import net.sf.jsqlparser.expression.Function;
import solr.jdbc.SolrColumn;


public class FunctionSolrColumn extends SolrColumn {
	private final String functionName;

	public FunctionSolrColumn(Function function) {
		columnName = function.toString();
		functionName = function.getName();
	}

	public String getFunctionName() {
		return functionName;
	}


}
