package com.google.code.solr_jdbc.extension.s2jdbc.types;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.apache.commons.lang.StringUtils;
import org.seasar.extension.jdbc.types.AbstractValueType;
import org.seasar.extension.jdbc.types.ValueTypes;
import org.seasar.extension.jdbc.util.BindVariableUtil;

public class ArrayType extends AbstractValueType {
	public ArrayType() {
		super(Types.ARRAY);
	}
	
	@Override
	public Object getValue(ResultSet resultSet, int index) throws SQLException{
		return (Object[])resultSet.getArray(index).getArray();
	}
	
	@Override
	public Object getValue(ResultSet resultSet, String columnName)
		throws SQLException {
		return (Object[])resultSet.getArray(columnName).getArray();
	}
	
	@Override
	public void bindValue(PreparedStatement ps, int index, Object value) throws SQLException {
		if (value == null) {
			setNull(ps, index);
		} else {
			ps.setObject(index, value);
		}
	}
	
	@Override
	public void bindValue(CallableStatement cs, String parameterName,
			Object value) throws SQLException {
		// TODO
	}

	@Override
	public Object getValue(CallableStatement cs, int index) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object getValue(CallableStatement cs, String parameterName)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String toText(Object value) {
		if(value == null) {
			return BindVariableUtil.nullText();
		}
		if(!value.getClass().isArray()) {
			return "<not array>";
		}
		Object[] array = (Object[]) value;
		return "[" + StringUtils.join(array, ',');
	}


}
