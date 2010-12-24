package com.google.code.solr_jdbc.expression;

import com.google.code.solr_jdbc.value.SolrType;
import com.google.code.solr_jdbc.value.SolrValue;
import com.google.code.solr_jdbc.value.ValueNull;

public class Parameter implements Item{
	private SolrValue value;
	private int index;
	private boolean needsLikeEscape;
	private String likeEscapeChar = "%";
	
	public Parameter(int index) {
		this.index = index;
	}
	
	public void setValue(SolrValue value) {
		this.value = value;
	}
	
	public SolrValue getValue() {
		if(value == null) {
			return ValueNull.INSTANCE;
		}
		return value;
	}
	
	public SolrType getType() {
		if (value != null) {
			return value.getType();
		}
		// FIXME Unknown
		return SolrType.STRING;
	}
	
	public String getQueryString() {
		if(value != null) {
			if(needsLikeEscape) {
				String strValue = value.getString();
				return strValue.replaceAll(likeEscapeChar, "*");
			} else {
				return value.getQueryString();
			}
		}
		return "";
	}
	public int getIndex() {
		return index;
	}

	public void setNeedsLikeEscape() {
		this.needsLikeEscape = true;
	}

	public void setLikeEscapeChar(String likeEscapeChar) {
		this.likeEscapeChar = likeEscapeChar;
	}
	
}
