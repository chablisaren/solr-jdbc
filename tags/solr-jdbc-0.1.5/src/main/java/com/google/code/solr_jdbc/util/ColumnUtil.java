package com.google.code.solr_jdbc.util;

import com.google.code.solr_jdbc.value.DataType;

public class ColumnUtil {
	public static DataType getColumnType(String columnName) {
		if (columnName == null) {
			return null;
		}
		int idx = columnName.lastIndexOf('.');
		String typeName = columnName.substring(idx);
		return DataType.getTypeByName(typeName.toUpperCase());
	}
}
