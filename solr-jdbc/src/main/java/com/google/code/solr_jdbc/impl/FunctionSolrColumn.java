package com.google.code.solr_jdbc.impl;

import com.google.code.solr_jdbc.expression.Expression;
import com.google.code.solr_jdbc.expression.SolrColumn;

import net.sf.jsqlparser.expression.Function;


public class FunctionSolrColumn extends Expression {
	private final String functionName;

	public FunctionSolrColumn(Function function) {
		columnName = function.toString();
		functionName = function.getName();
	}

	public String getFunctionName() {
		return functionName;
	}


}
