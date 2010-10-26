package solr.jdbc.value;

import java.math.BigDecimal;
import java.sql.Date;


public abstract class SolrValue {

	public SolrType getType() {
		return null;
	}

	public abstract Object getObject();
	public abstract String getString();
	public abstract String getQueryString();

	public Boolean getBoolean() {
		return ((ValueBoolean)convertTo(SolrType.BOOLEAN)).getBoolean();
	}

	public Date getDate() {
		return ((ValueDate)convertTo(SolrType.DATE)).getDate();
	}

	public Date getDateNoCopy() {
		return ((ValueDate)convertTo(SolrType.DATE)).getDateNoCopy();
	}

	public BigDecimal getBigDecimal() {
		return ((ValueDecimal)convertTo(SolrType.DECIMAL)).getBigDecimal();
	}

	public int getInt() {
		return ((ValueInt) convertTo(SolrType.INT)).getInt();
	}

	public SolrValue convertTo(SolrType type) {
		if(getType() == type) {
			return this;
		}
		String s = getString();
		switch(type) {
		case BOOLEAN: {
			return ValueBoolean.get(getSignum() != 0);
		}
		case INT: {
			return ValueInt.get(Integer.parseInt(s.trim()));
		}
		case LONG: {
			return ValueLong.get(Long.parseLong(s.trim()));
		}
		case DECIMAL: {
			return ValueDecimal.get(new BigDecimal(s.trim()));
		}
		case DATE: {
			return ValueDate.get(ValueDate.parseDate(s.trim()));
		}
		case STRING: {
			return ValueString.get(s);
		}
		case ARRAY: {
			return ValueArray.get(new SolrValue[]{ValueString.get(s)});
		}
		default:
			throw new RuntimeException("データ変換エラー");
		}
	}

	public int getSignum() {
		throw new UnsupportedOperationException();
	}
}
