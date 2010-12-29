package com.google.code.solr_jdbc.expression;

import com.google.code.solr_jdbc.value.SolrType;
import com.google.code.solr_jdbc.value.SolrValue;
import com.google.code.solr_jdbc.value.ValueNull;

/**
 * Expression of Value.
 * 
 * This class stands for "SELECT 1, 'money',..."
 * 
 * @author kawasima
 *
 */
public class ValueExpression extends Expression {
	private final SolrValue value;
	private static final ValueExpression NULL = new ValueExpression(ValueNull.INSTANCE);
	
	private ValueExpression(SolrValue value) {
		this.value = value;
	}
	
	public static ValueExpression get(SolrValue value) {
		if (value == ValueNull.INSTANCE) {
			return NULL;
		}
		return new ValueExpression(value);
	}

	public SolrValue getValue() {
		return value;
	}
	
	@Override
	public SolrType getType() {
		return value.getType();
	}
}
