package com.google.code.solr_jdbc.expression;

import com.google.code.solr_jdbc.value.SolrValue;

public class Literal implements Item {

	private SolrValue value;
	
	public Literal(SolrValue value) {
		this.value = value;
	}
	@Override
	public SolrValue getValue() {
		return value;
	}

}
