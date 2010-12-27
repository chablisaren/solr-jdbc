package com.google.code.solr_jdbc.expression;

import org.apache.commons.lang.StringUtils;

import com.google.code.solr_jdbc.SolrColumn;
import com.google.code.solr_jdbc.message.DbException;
import com.google.code.solr_jdbc.message.ErrorCode;
import com.google.code.solr_jdbc.value.SolrType;
import com.google.code.solr_jdbc.value.SolrValue;
import com.google.code.solr_jdbc.value.ValueNull;

public class Parameter implements Item{
	private SolrValue value;
	private int index;
	private boolean needsLikeEscape;
	private String likeEscapeChar = "%";
	private SolrColumn targetColumn;
	
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
	
	public void setColumn(SolrColumn column) {
		this.targetColumn = column;
	}
	public SolrType getType() {
		if (value != null) {
			return value.getType();
		}
		// FIXME Unknown
		return SolrType.STRING;
	}
	
	public String getQueryString() {
		if (value != null) {
			if (needsLikeEscape) {
				String strValue = value.getString();
				if (targetColumn != null && targetColumn.getType() == SolrType.TEXT) {
					if (strValue.startsWith(likeEscapeChar)
							&& strValue.endsWith(likeEscapeChar)) {
						strValue = StringUtils.strip(strValue, likeEscapeChar);
					}
				}
				if (strValue.startsWith(likeEscapeChar)) {
					// TODO 専用のメッセージを作る
					throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED,
							"この型では中間一致・後方一致検索はできません");
				}
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
