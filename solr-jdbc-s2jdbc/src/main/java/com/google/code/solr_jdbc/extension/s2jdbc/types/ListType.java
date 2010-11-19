package com.google.code.solr_jdbc.extension.s2jdbc.types;

import java.sql.Array;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.seasar.extension.jdbc.types.AbstractValueType;
import org.seasar.extension.jdbc.util.BindVariableUtil;

public class ListType extends AbstractValueType {
	public ListType() {
		super(Types.ARRAY);
	}
	
	@Override
	public Object getValue(ResultSet resultSet, int index) throws SQLException {
		Array array = resultSet.getArray(index);
		if(array == null) {
			return null;
		}
		Object[] value = (Object[])array.getArray();
		if (value == null) {
			return null;
		}
		return Arrays.asList(value);
	}

	@Override
	public Object getValue(ResultSet resultSet, String columnName)
			throws SQLException {
		Array array = resultSet.getArray(columnName);
		if (array == null) {
			return null;
		}
		Object[] value = (Object[])array.getArray();
		if (value == null) {
			return null;
		}
		return Arrays.asList(array);
	}

	@Override
	public Object getValue(CallableStatement cs, int index) throws SQLException {
		// TODO
		return null;
	}

	@Override
	public Object getValue(CallableStatement cs, String parameterName)
			throws SQLException {
		// TODO
		return null;
	}


	@Override
	public void bindValue(PreparedStatement ps, int index, Object value)
			throws SQLException {
		if  (value == null) {
			setNull(ps, index);
		} else {
			ps.setObject(index, ((List)value).toArray());
		}

	}

	@Override
	public void bindValue(CallableStatement cs, String parameterName,
			Object value) throws SQLException {
		// TODO
	}


	@Override
	public String toText(Object value) {
		if(value == null) {
			return BindVariableUtil.nullText();
		}
		if(!List.class.isAssignableFrom(value.getClass())) {
			return "<not List>";  
		}
		return "[" + StringUtils.join((Collection)value, ',') + "]";
	}

}
