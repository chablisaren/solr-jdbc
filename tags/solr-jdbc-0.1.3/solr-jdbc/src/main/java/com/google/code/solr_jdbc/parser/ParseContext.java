package com.google.code.solr_jdbc.parser;

public enum ParseContext {
	NONE,
	EQUAL,
	NOT_EQUAL,
	GREATER_THAN,
	GREATER_THAN_EQUAL,
	MINOR_THAN,
	MINOR_THAN_EQUAL,
	IN,
	BETWEEN,
	LIKE
}
